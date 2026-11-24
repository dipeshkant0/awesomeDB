#![allow(
    dead_code,
    unused_variables,
    unused_imports,
    unused_mut,
    unreachable_code
)]

use super::Operator;
use super::io_utils::{
    AlternateHasher, BloomFilter, FastMap, Fnv1aHasher, ScratchRun, ScratchRunReader,
    ScratchRunWriter, SharedBufferManager,
};
use crate::buffer_pool::BufferPoolManager;
use crate::data::{Row, Value};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};

const MIN_OPERATOR_MEMORY_BYTES: usize = 1 * 1024 * 1024;
const MIN_CROSS_STREAM_CHUNK_BYTES: usize = 2 * 1024 * 1024;
const CROSS_SPILL_WRITER_TARGET_BYTES: usize = 1 * 1024 * 1024;
const BLOOM_FILTER_BYTES: usize = 128 * 1024;
const GRACE_HASH_MIN_PARTITIONS: usize = 8;
const GRACE_HASH_MAX_PARTITIONS: usize = 64;

#[inline(always)]
fn scratch_writer_blocks(block_size: usize) -> usize {
    ((CROSS_SPILL_WRITER_TARGET_BYTES + block_size - 1) / block_size).max(1)
}

pub struct BufferedOperator<'a> {
    stolen_rows: std::vec::IntoIter<Row>,
    child: Box<dyn Operator + 'a>,
}

impl<'a> BufferedOperator<'a> {
    pub fn new(stolen_rows: Vec<Row>, child: Box<dyn Operator + 'a>) -> Self {
        Self {
            stolen_rows: stolen_rows.into_iter(),
            child,
        }
    }
}

impl<'a> Operator for BufferedOperator<'a> {
    #[inline(always)]
    fn next(&mut self) -> Option<Row> {
        if let Some(row) = self.stolen_rows.next() {
            return Some(row);
        }
        self.child.next()
    }
}

pub struct InMemoryHashJoinOperator<'a> {
    left_child: Box<dyn Operator + 'a>,
    build_rows: Vec<Row>,
    left_col_idx: usize,
    right_col_idx: usize,
    hash_table: FastMap<Value, Vec<usize>>,
    bloom_filter: BloomFilter,
    left_row: Option<Row>,
    match_indices: Vec<usize>,
    match_idx: usize,
    initialized: bool,
}

impl<'a> InMemoryHashJoinOperator<'a> {
    pub fn new(left: Box<dyn Operator + 'a>, build_rows: Vec<Row>, left_col_idx: usize, right_col_idx: usize) -> Self {
        // Scale bloom filter proportional to build size:
        // - Tiny builds (< 1K rows): 8KB bloom is sufficient
        // - Small builds (< 10K rows): 32KB
        // - Normal builds: 64KB
        // Avoids wasting 128KB on tiny builds under 64MB RLIMIT_AS.
        let bloom_bytes = if build_rows.len() < 1_000 {
            8 * 1024
        } else if build_rows.len() < 10_000 {
            32 * 1024
        } else {
            64 * 1024
        };
        Self {
            left_child: left,
            build_rows,
            left_col_idx,
            right_col_idx,
            hash_table: FastMap::default(),
            bloom_filter: BloomFilter::new(bloom_bytes),
            left_row: None,
            match_indices: Vec::new(),
            match_idx: 0,
            initialized: false,
        }
    }
}

impl<'a> Operator for InMemoryHashJoinOperator<'a> {
    fn next(&mut self) -> Option<Row> {
        if !self.initialized {
            for (idx, row) in self.build_rows.iter().enumerate() {
                let key = &row.values[self.right_col_idx];
                let mut h1 = Fnv1aHasher::default();
                let mut h2 = AlternateHasher::default();
                key.hash(&mut h1);
                key.hash(&mut h2);
                self.bloom_filter.insert(h1.finish(), h2.finish());
                self.hash_table.entry(key.clone()).or_insert_with(Vec::new).push(idx);
            }
            self.hash_table.shrink_to_fit();
            self.initialized = true;
        }

        if self.match_idx < self.match_indices.len() {
            let left = self.left_row.as_ref().unwrap();
            let right = &self.build_rows[self.match_indices[self.match_idx]];
            self.match_idx += 1;
            return Some(Row::combine(left, right));
        }

        loop {
            self.match_indices.clear();
            self.match_idx = 0;

            self.left_row = self.left_child.next();
            let left = self.left_row.as_ref()?;
            let key = &left.values[self.left_col_idx];

            let mut h1 = Fnv1aHasher::default();
            let mut h2 = AlternateHasher::default();
            key.hash(&mut h1);
            key.hash(&mut h2);

            if !self.bloom_filter.contains(h1.finish(), h2.finish()) {
                continue;
            }

            if let Some(matches) = self.hash_table.get(key) {
                self.match_indices.extend_from_slice(matches);
                let right = &self.build_rows[self.match_indices[0]];
                self.match_idx = 1;
                return Some(Row::combine(left, right));
            }
        }
    }
}

struct ScratchRunScanOperator<'a, R: Read, W: Write> {
    reader: ScratchRunReader<'a, R, W>,
}
impl<'a, R: Read, W: Write> ScratchRunScanOperator<'a, R, W> {
    fn new(
        pool_ptr: *mut BufferPoolManager<R, W>,
        run: &ScratchRun,
        block_size: usize,
        free_on_drop: bool,
    ) -> Self {
        Self {
            reader: ScratchRunReader::new(
                pool_ptr,
                run.spans.clone(),
                run.total_bytes,
                block_size,
                free_on_drop,
            ),
        }
    }
}
impl<'a, R: Read, W: Write> Operator for ScratchRunScanOperator<'a, R, W> {
    #[inline(always)]
    fn next(&mut self) -> Option<Row> {
        self.reader.read_row()
    }
}

struct PartitionHashJoinState<'a, R: Read, W: Write> {
    probe_reader: ScratchRunReader<'a, R, W>,
    build_rows: Vec<Row>,
    hash_table: FastMap<Value, Vec<usize>>,
    build_key_idx: usize,
    probe_key_idx: usize,
    build_is_right: bool,
    current_probe_row: Option<Row>,
    current_match_indices: Vec<usize>,
    match_pos: usize,
}

impl<'a, R: Read, W: Write> PartitionHashJoinState<'a, R, W> {
    fn new(
        probe_reader: ScratchRunReader<'a, R, W>,
        build_rows: Vec<Row>,
        build_key_idx: usize,
        probe_key_idx: usize,
        build_is_right: bool,
    ) -> Self {
        Self {
            probe_reader,
            build_rows,
            hash_table: FastMap::default(),
            build_key_idx,
            probe_key_idx,
            build_is_right,
            current_probe_row: None,
            current_match_indices: Vec::new(),
            match_pos: 0,
        }
    }

    fn init_hash_table(&mut self) {
        for (idx, row) in self.build_rows.iter().enumerate() {
            let key = &row.values[self.build_key_idx];
            self.hash_table
                .entry(key.clone())
                .or_insert_with(Vec::new)
                .push(idx);
        }
        self.hash_table.shrink_to_fit();
    }

    fn next_row(&mut self) -> Option<Row> {
        if self.hash_table.is_empty() && !self.build_rows.is_empty() {
            self.init_hash_table();
        }

        if self.match_pos < self.current_match_indices.len() {
            let probe = self.current_probe_row.as_ref().unwrap();
            let build = &self.build_rows[self.current_match_indices[self.match_pos]];
            self.match_pos += 1;
            return Some(if self.build_is_right {
                Row::combine(probe, build)
            } else {
                Row::combine(build, probe)
            });
        }

        loop {
            self.current_match_indices.clear();
            self.match_pos = 0;
            self.current_probe_row = self.probe_reader.read_row();
            let probe = self.current_probe_row.as_ref()?;
            let key = &probe.values[self.probe_key_idx];
            if let Some(matches) = self.hash_table.get(key) {
                self.current_match_indices.extend_from_slice(matches);
                let build = &self.build_rows[self.current_match_indices[0]];
                self.match_pos = 1;
                return Some(if self.build_is_right {
                    Row::combine(probe, build)
                } else {
                    Row::combine(build, probe)
                });
            }
        }
    }
}

pub struct GraceHashJoinOperator<'a, R: Read + 'a, W: Write + 'a> {
    left_child: Option<Box<dyn Operator + 'a>>,
    right_child: Option<Box<dyn Operator + 'a>>,
    left_col_idx: usize,
    right_col_idx: usize,
    memory_limit_bytes: usize,
    scratch_pool_ptr: *mut BufferPoolManager<R, W>,
    scratch_block_size: usize,
    initialized: bool,
    partitions_left: Vec<ScratchRun>,
    partitions_right: Vec<ScratchRun>,
    partition_idx: usize,
    active_hash: Option<PartitionHashJoinState<'a, R, W>>,
    active_fallback: Option<Box<dyn Operator + 'a>>,
}

impl<'a, R: Read + 'a, W: Write + 'a> GraceHashJoinOperator<'a, R, W> {
    pub fn new(
        left: Box<dyn Operator + 'a>,
        right: Box<dyn Operator + 'a>,
        left_col_idx: usize,
        right_col_idx: usize,
        memory_limit_bytes: usize,
        scratch_pool_ptr: *mut BufferPoolManager<R, W>,
        scratch_block_size: usize,
    ) -> Self {
        let bounded = memory_limit_bytes.max(MIN_OPERATOR_MEMORY_BYTES);
        Self {
            left_child: Some(left),
            right_child: Some(right),
            left_col_idx,
            right_col_idx,
            memory_limit_bytes: bounded,
            scratch_pool_ptr,
            scratch_block_size,
            initialized: false,
            partitions_left: Vec::new(),
            partitions_right: Vec::new(),
            partition_idx: 0,
            active_hash: None,
            active_fallback: None,
        }
    }

    fn choose_partitions(&self) -> usize {
        let m = self.memory_limit_bytes;
        let mut p = if m >= 8 * 1024 * 1024 {
            64
        } else if m >= 4 * 1024 * 1024 {
            32
        } else if m >= 2 * 1024 * 1024 {
            16
        } else {
            8
        };
        p = p.clamp(GRACE_HASH_MIN_PARTITIONS, GRACE_HASH_MAX_PARTITIONS);
        p.next_power_of_two()
    }

    fn partition_child(
        &self,
        mut child: Box<dyn Operator + 'a>,
        key_idx: usize,
        num_partitions: usize,
    ) -> Vec<ScratchRun> {
        let flush_threshold = (self.memory_limit_bytes / 4)
            .clamp(512 * 1024, 4 * 1024 * 1024)
            .max(self.scratch_block_size);
        let mut bm = SharedBufferManager::new(
            self.scratch_pool_ptr,
            num_partitions,
            self.scratch_block_size,
            flush_threshold,
        );
        while let Some(row) = child.next() {
            let key = &row.values[key_idx];
            let mut h = Fnv1aHasher::default();
            key.hash(&mut h);
            let p = (h.finish() as usize) & (num_partitions - 1);
            bm.write_row(p, &row);
        }
        bm.finish()
    }

    fn free_run(&self, run: &ScratchRun) {
        let pool = unsafe { &mut *self.scratch_pool_ptr };
        for span in &run.spans {
            for i in 0..span.num_blocks {
                pool.disk_manager
                    .free_anon_block(span.start_block + (i as u64));
            }
        }
    }

    fn start_next_partition(&mut self) {
        self.active_hash = None;
        self.active_fallback = None;

        while self.partition_idx < self.partitions_left.len() {
            let left_run = &self.partitions_left[self.partition_idx];
            let right_run = &self.partitions_right[self.partition_idx];
            self.partition_idx += 1;

            if left_run.total_bytes == 0 || right_run.total_bytes == 0 {
                self.free_run(left_run);
                self.free_run(right_run);
                continue;
            }

            let build_is_right = right_run.total_bytes <= left_run.total_bytes;
            let build_run = if build_is_right { right_run } else { left_run };
            let probe_run = if build_is_right { left_run } else { right_run };

            let (build_key_idx, probe_key_idx) = if build_is_right {
                (self.right_col_idx, self.left_col_idx)
            } else {
                (self.left_col_idx, self.right_col_idx)
            };

            let mut build_reader = ScratchRunReader::new(
                self.scratch_pool_ptr,
                build_run.spans.clone(),
                build_run.total_bytes,
                self.scratch_block_size,
                false,
            );
            let mut build_rows = Vec::new();
            let mut mem = 0usize;
            let mut overflow = false;
            while let Some(r) = build_reader.read_row() {
                mem += r.encoded_len() + 96;
                build_rows.push(r);
                if mem > self.memory_limit_bytes {
                    overflow = true;
                    break;
                }
            }

            if !overflow {
                let probe_reader = ScratchRunReader::new(
                    self.scratch_pool_ptr,
                    probe_run.spans.clone(),
                    probe_run.total_bytes,
                    self.scratch_block_size,
                    false, 
                );
                self.active_hash = Some(PartitionHashJoinState::new(
                    probe_reader,
                    build_rows,
                    build_key_idx,
                    probe_key_idx,
                    build_is_right,
                ));
                return;
            }

            // Build side overflows memory — free the partial build_rows immediately
            // to reclaim memory before creating sort operators.
            drop(build_rows);

            let left_scan: Box<dyn Operator + 'a> = Box::new(ScratchRunScanOperator::new(
                self.scratch_pool_ptr,
                left_run,
                self.scratch_block_size,
                false,
            ));
            let right_scan: Box<dyn Operator + 'a> = Box::new(ScratchRunScanOperator::new(
                self.scratch_pool_ptr,
                right_run,
                self.scratch_block_size,
                false,
            ));

            let l_sort: Box<dyn Operator + 'a> = Box::new(crate::operators::sort::SortOperator::new(
                left_scan,
                vec![(self.left_col_idx, true)],
                self.memory_limit_bytes,
                self.scratch_pool_ptr,
                self.scratch_block_size,
            ));
            let r_sort: Box<dyn Operator + 'a> = Box::new(crate::operators::sort::SortOperator::new(
                right_scan,
                vec![(self.right_col_idx, true)],
                self.memory_limit_bytes,
                self.scratch_pool_ptr,
                self.scratch_block_size,
            ));

            self.active_fallback = Some(Box::new(SortMergeJoinOperator::new(
                l_sort,
                r_sort,
                self.left_col_idx,
                self.right_col_idx,
                self.scratch_pool_ptr,
                self.scratch_block_size,
                self.memory_limit_bytes,
            )));
            return;
        }
    }
}

// 🚀 CRITICAL DISK LEAK FIX: Free active partitions if dropped early
impl<'a, R: Read + 'a, W: Write + 'a> Drop for GraceHashJoinOperator<'a, R, W> {
    fn drop(&mut self) {
        let start = if self.active_hash.is_some() || self.active_fallback.is_some() {
            self.partition_idx.saturating_sub(1)
        } else {
            self.partition_idx
        };
        let pool = unsafe { &mut *self.scratch_pool_ptr };
        for i in start..self.partitions_left.len() {
            for span in &self.partitions_left[i].spans {
                for j in 0..span.num_blocks {
                    pool.disk_manager.free_anon_block(span.start_block + (j as u64));
                }
            }
            for span in &self.partitions_right[i].spans {
                for j in 0..span.num_blocks {
                    pool.disk_manager.free_anon_block(span.start_block + (j as u64));
                }
            }
        }
    }
}

impl<'a, R: Read + 'a, W: Write + 'a> Operator for GraceHashJoinOperator<'a, R, W> {
    fn next(&mut self) -> Option<Row> {
        if !self.initialized {
            let num_partitions = self.choose_partitions();
            let left = self.left_child.take().unwrap();
            let right = self.right_child.take().unwrap();
            self.partitions_left = self.partition_child(left, self.left_col_idx, num_partitions);
            self.partitions_right = self.partition_child(right, self.right_col_idx, num_partitions);
            self.partition_idx = 0;
            self.initialized = true;
            self.start_next_partition();
        }

        loop {
            if let Some(h) = self.active_hash.as_mut() {
                if let Some(row) = h.next_row() {
                    return Some(row);
                }
                let done_idx = self.partition_idx.saturating_sub(1);
                if done_idx < self.partitions_left.len() {
                    self.free_run(&self.partitions_left[done_idx]);
                    self.free_run(&self.partitions_right[done_idx]);
                }
                self.start_next_partition();
                continue;
            }

            if let Some(op) = self.active_fallback.as_mut() {
                if let Some(row) = op.next() {
                    return Some(row);
                }
                let done_idx = self.partition_idx.saturating_sub(1);
                if done_idx < self.partitions_left.len() {
                    self.free_run(&self.partitions_left[done_idx]);
                    self.free_run(&self.partitions_right[done_idx]);
                }
                self.active_fallback = None;
                self.start_next_partition();
                continue;
            }

            return None;
        }
    }
}

pub struct SortMergeJoinOperator<'a, R: Read, W: Write> {
    left_child: Box<dyn Operator + 'a>,
    right_child: Box<dyn Operator + 'a>,
    left_col_idx: usize,
    right_col_idx: usize,
    left_row: Option<Row>,
    right_row: Option<Row>,
    right_buffer: Vec<Row>,
    right_buffer_idx: usize,
    match_key: Option<Value>,
    initialized: bool,
    spilled_run: Option<ScratchRun>,
    spilled_reader: Option<ScratchRunReader<'a, R, W>>,
    scratch_pool_ptr: *mut BufferPoolManager<R, W>,
    scratch_block_size: usize,
    memory_limit_bytes: usize,
}

impl<'a, R: Read, W: Write> Drop for SortMergeJoinOperator<'a, R, W> {
    fn drop(&mut self) {
        if let Some(run) = &self.spilled_run {
            let pool = unsafe { &mut *self.scratch_pool_ptr };
            for span in &run.spans {
                for i in 0..span.num_blocks {
                    pool.disk_manager
                        .free_anon_block(span.start_block + (i as u64));
                }
            }
        }
    }
}

impl<'a, R: Read, W: Write> SortMergeJoinOperator<'a, R, W> {
    pub fn new(
        left: Box<dyn Operator + 'a>,
        right: Box<dyn Operator + 'a>,
        left_col_idx: usize,
        right_col_idx: usize,
        scratch_pool_ptr: *mut BufferPoolManager<R, W>,
        scratch_block_size: usize,
        memory_limit_bytes: usize,
    ) -> Self {
        Self {
            left_child: left,
            right_child: right,
            left_col_idx,
            right_col_idx,
            left_row: None,
            right_row: None,
            right_buffer: Vec::new(),
            right_buffer_idx: 0,
            match_key: None,
            initialized: false,
            spilled_run: None,
            spilled_reader: None,
            scratch_pool_ptr,
            scratch_block_size,
            memory_limit_bytes,
        }
    }

    #[inline(always)]
    fn compare_values_ord(lhs: &Value, rhs: &Value) -> Ordering {
        lhs.partial_cmp(rhs).unwrap_or(Ordering::Equal)
    }
}

impl<'a, R: Read, W: Write> Operator for SortMergeJoinOperator<'a, R, W> {
    fn next(&mut self) -> Option<Row> {
        if !self.initialized {
            self.left_row = self.left_child.next();
            self.right_row = self.right_child.next();
            self.initialized = true;
        }

        loop {
            if self.right_buffer_idx < self.right_buffer.len() {
                let left = self.left_row.as_ref().unwrap();
                let right = &self.right_buffer[self.right_buffer_idx];
                self.right_buffer_idx += 1;
                return Some(Row::combine(left, right));
            }

            if let Some(reader) = self.spilled_reader.as_mut() {
                if let Some(right) = reader.read_row() {
                    let left = self.left_row.as_ref().unwrap();
                    return Some(Row::combine(left, &right));
                }
            }

            if !self.right_buffer.is_empty() || self.spilled_run.is_some() {
                self.left_row = self.left_child.next();
                if let Some(left) = &self.left_row {
                    let left_key = &left.values[self.left_col_idx];
                    if Some(left_key) == self.match_key.as_ref() {
                        self.right_buffer_idx = 0;
                        if let Some(run) = &self.spilled_run {
                            self.spilled_reader = Some(ScratchRunReader::new(
                                self.scratch_pool_ptr,
                                run.spans.clone(),
                                run.total_bytes,
                                self.scratch_block_size,
                                false 
                            ));
                        }
                        continue;
                    }
                }
                
                self.right_buffer.clear();
                if let Some(run) = self.spilled_run.take() {
                    let pool = unsafe { &mut *self.scratch_pool_ptr };
                    for span in &run.spans {
                        for i in 0..span.num_blocks {
                            pool.disk_manager
                                .free_anon_block(span.start_block + (i as u64));
                        }
                    }
                }
                self.spilled_reader = None;
                self.match_key = None;
            }

            let left = self.left_row.as_ref()?;
            let right = self.right_row.as_ref()?;
            let left_key = &left.values[self.left_col_idx];
            let right_key = &right.values[self.right_col_idx];

            match Self::compare_values_ord(left_key, right_key) {
                Ordering::Less => self.left_row = self.left_child.next(),
                Ordering::Greater => self.right_row = self.right_child.next(),
                Ordering::Equal => {
                    self.match_key = Some(left_key.clone());
                    
                    let mut mem_usage = 0;
                    let mut writer: Option<ScratchRunWriter<R, W>> = None;

                    self.right_buffer.push(right.clone());
                    mem_usage += right.encoded_len();
                    self.right_row = self.right_child.next();

                    while let Some(r) = &self.right_row {
                        if &r.values[self.right_col_idx] == left_key {
                            if writer.is_none() && mem_usage + r.encoded_len() > self.memory_limit_bytes {
                                writer = Some(ScratchRunWriter::new(
                                    self.scratch_pool_ptr,
                                    self.scratch_block_size,
                                    scratch_writer_blocks(self.scratch_block_size),
                                ));
                            }

                            if let Some(w) = writer.as_mut() {
                                w.write_row(r); 
                            } else {
                                self.right_buffer.push(r.clone());
                                mem_usage += r.encoded_len();
                            }
                            self.right_row = self.right_child.next();
                        } else {
                            break;
                        }
                    }

                    if let Some(w) = writer {
                        let run = w.finish();
                        self.spilled_reader = Some(ScratchRunReader::new(
                            self.scratch_pool_ptr,
                            run.spans.clone(),
                            run.total_bytes,
                            self.scratch_block_size,
                            false,
                        ));
                        self.spilled_run = Some(run);
                    }

                    self.right_buffer_idx = 0;
                }
            }
        }
    }
}

pub struct AdaptiveJoinOperator<'a, R: Read + 'a, W: Write + 'a> {
    left_child: Option<Box<dyn Operator + 'a>>,
    right_child: Option<Box<dyn Operator + 'a>>,
    left_col_idx: usize,
    right_col_idx: usize,
    sort_memory_limit: usize,
    left_est_rows: f64,
    right_est_rows: f64,
    scratch_pool_ptr: *mut BufferPoolManager<R, W>,
    scratch_block_size: usize,
    active_strategy: Option<Box<dyn Operator + 'a>>,
}

impl<'a, R: Read + 'a, W: Write + 'a> AdaptiveJoinOperator<'a, R, W> {
    pub fn new(
        left: Box<dyn Operator + 'a>,
        right: Box<dyn Operator + 'a>,
        left_col_idx: usize,
        right_col_idx: usize,
        sort_memory_limit: usize,
        left_est_rows: f64,
        right_est_rows: f64,
        scratch_pool_ptr: *mut BufferPoolManager<R, W>,
        scratch_block_size: usize,
    ) -> Self {
        Self {
            left_child: Some(left),
            right_child: Some(right),
            left_col_idx,
            right_col_idx,
            sort_memory_limit,
            left_est_rows,
            right_est_rows,
            scratch_pool_ptr,
            scratch_block_size,
            active_strategy: None,
        }
    }

    #[inline(always)]
    fn estimate_row_size(&self, row: &Row) -> usize {
        row.encoded_len() + 96
    }
}

impl<'a, R: Read + 'a, W: Write + 'a> Operator for AdaptiveJoinOperator<'a, R, W> {
    fn next(&mut self) -> Option<Row> {
        if self.active_strategy.is_none() {
            let mut right_child = self.right_child.take().unwrap();
            let mut buffered_rows = Vec::with_capacity(1024);
            let mut current_memory = 0;
            let mut sample_bytes = 0usize;
            let mut sample_rows = 0usize;

            let max_memory = ((self.sort_memory_limit * 5) / 8).max(1 * 1024 * 1024);
            let mut memory_breached = false;

            while let Some(row) = right_child.next() {
                current_memory += self.estimate_row_size(&row);
                if sample_rows < 256 {
                    sample_bytes += row.encoded_len();
                    sample_rows += 1;
                }
                buffered_rows.push(row);
                if current_memory > max_memory {
                    memory_breached = true;
                    break;
                }
            }
            if !memory_breached {
                buffered_rows.shrink_to_fit();
            }

            let left_child = self.left_child.take().unwrap();
            if !memory_breached {
                self.active_strategy = Some(Box::new(InMemoryHashJoinOperator::new(
                    left_child,
                    buffered_rows,
                    self.left_col_idx,
                    self.right_col_idx,
                )));
            } else {
                let heavy_right: Box<dyn Operator + 'a> =
                    Box::new(BufferedOperator::new(buffered_rows, right_child));

                let avg_row_bytes = if sample_rows == 0 {
                    64.0
                } else {
                    (sample_bytes as f64 / sample_rows as f64).max(8.0)
                };
                let est_input_bytes =
                    (self.left_est_rows + self.right_est_rows).max(1.0) * avg_row_bytes;

                let use_grace_hash = est_input_bytes > (self.sort_memory_limit as f64 * 4.0);

                if use_grace_hash {
                    self.active_strategy = Some(Box::new(GraceHashJoinOperator::new(
                        left_child,
                        heavy_right,
                        self.left_col_idx,
                        self.right_col_idx,
                        self.sort_memory_limit,
                        self.scratch_pool_ptr,
                        self.scratch_block_size,
                    )));
                } else {
                    let l_sort: Box<dyn Operator + 'a> =
                        Box::new(crate::operators::sort::SortOperator::new(
                            left_child,
                            vec![(self.left_col_idx, true)],
                            self.sort_memory_limit / 2,
                            self.scratch_pool_ptr,
                            self.scratch_block_size,
                        ));
                    let r_sort: Box<dyn Operator + 'a> =
                        Box::new(crate::operators::sort::SortOperator::new(
                            heavy_right,
                            vec![(self.right_col_idx, true)],
                            self.sort_memory_limit / 2,
                            self.scratch_pool_ptr,
                            self.scratch_block_size,
                        ));
                    self.active_strategy = Some(Box::new(SortMergeJoinOperator::new(
                        l_sort,
                        r_sort,
                        self.left_col_idx,
                        self.right_col_idx,
                        self.scratch_pool_ptr,
                        self.scratch_block_size,
                        self.sort_memory_limit,
                    )));
                }
            }
        }
        self.active_strategy.as_mut().unwrap().next()
    }
}

pub struct CrossOperator<'a, R: Read, W: Write> {
    left_child: Option<Box<dyn Operator + 'a>>,
    right_child: Option<Box<dyn Operator + 'a>>,
    materialize_left: bool,
    scratch_pool_ptr: *mut BufferPoolManager<R, W>,
    scratch_block_size: usize,
    in_memory_materialized: Vec<Row>,
    materialized_spilled: bool,
    spilled_run: Option<ScratchRun>,
    stream_chunk: Vec<Row>,
    stream_chunk_idx: usize,
    current_reader: Option<ScratchRunReader<'a, R, W>>,
    current_spilled_row: Option<Row>,
    stream_exhausted: bool,
    initialized: bool,
    current_stream_row: Option<Row>,
    materialized_idx: usize,
    memory_limit_bytes: usize,
    stream_chunk_limit_bytes: usize,
}

impl<'a, R: Read, W: Write> CrossOperator<'a, R, W> {
    pub fn new(
        left: Box<dyn Operator + 'a>,
        right: Box<dyn Operator + 'a>,
        mat_left: bool,
        pool_ptr: *mut BufferPoolManager<R, W>,
        block_size: usize,
        memory_limit_bytes: usize,
    ) -> Self {
        let bounded_limit = memory_limit_bytes.max(MIN_OPERATOR_MEMORY_BYTES);
        Self {
            left_child: Some(left),
            right_child: Some(right),
            materialize_left: mat_left,
            scratch_pool_ptr: pool_ptr,
            scratch_block_size: block_size,
            in_memory_materialized: Vec::new(),
            materialized_spilled: false,
            spilled_run: None,
            stream_chunk: Vec::new(),
            stream_chunk_idx: 0,
            current_reader: None,
            current_spilled_row: None,
            stream_exhausted: false,
            initialized: false,
            current_stream_row: None,
            materialized_idx: 0,
            memory_limit_bytes: bounded_limit,
            stream_chunk_limit_bytes: bounded_limit
                .max(MIN_CROSS_STREAM_CHUNK_BYTES)
                .min(8 * 1024 * 1024),
        }
    }

    #[inline(always)]
    fn estimate_row_size(row: &Row) -> usize {
        row.encoded_len() + 96
    }
}

impl<'a, R: Read, W: Write> Drop for CrossOperator<'a, R, W> {
    fn drop(&mut self) {
        if let Some(run) = &self.spilled_run {
            let pool = unsafe { &mut *self.scratch_pool_ptr };
            for span in &run.spans {
                for i in 0..span.num_blocks {
                    pool.disk_manager
                        .free_anon_block(span.start_block + (i as u64));
                }
            }
        }
    }
}

impl<'a, R: Read, W: Write> Operator for CrossOperator<'a, R, W> {
    fn next(&mut self) -> Option<Row> {
        if !self.initialized {
            let mut child = if self.materialize_left {
                self.left_child.take().unwrap()
            } else {
                self.right_child.take().unwrap()
            };
            let mut mem_usage: usize = 0;
            let mut writer = None;

            while let Some(row) = child.next() {
                mem_usage = mem_usage.saturating_add(Self::estimate_row_size(&row));

                if !self.materialized_spilled && mem_usage >= self.memory_limit_bytes {
                    self.materialized_spilled = true;
                    let mut w = ScratchRunWriter::new(
                        self.scratch_pool_ptr,
                        self.scratch_block_size,
                        scratch_writer_blocks(self.scratch_block_size),
                    );
                    for r in &self.in_memory_materialized {
                        w.write_row(r);
                    }
                    self.in_memory_materialized = Vec::new();
                    writer = Some(w);
                }
                if self.materialized_spilled {
                    writer.as_mut().unwrap().write_row(&row);
                } else {
                    self.in_memory_materialized.push(row);
                }
            }
            if let Some(w) = writer {
                self.spilled_run = Some(w.finish());
            }
            self.initialized = true;
        }

        if !self.materialized_spilled {
            if self.in_memory_materialized.is_empty() {
                return None;
            }
            loop {
                if self.current_stream_row.is_none() {
                    let stream = if self.materialize_left {
                        self.right_child.as_mut().unwrap()
                    } else {
                        self.left_child.as_mut().unwrap()
                    };
                    self.current_stream_row = stream.next();
                    self.materialized_idx = 0;
                    if self.current_stream_row.is_none() {
                        return None;
                    }
                }
                if self.materialized_idx < self.in_memory_materialized.len() {
                    let mat_row = &self.in_memory_materialized[self.materialized_idx];
                    self.materialized_idx += 1;
                    let s_row = self.current_stream_row.as_ref().unwrap();
                    return Some(if self.materialize_left {
                        Row::combine(mat_row, s_row)
                    } else {
                        Row::combine(s_row, mat_row)
                    });
                } else {
                    self.current_stream_row = None;
                }
            }
        } else {
            if self.spilled_run.as_ref().unwrap().total_bytes == 0 {
                return None;
            }
            loop {
                if self.stream_chunk.is_empty() {
                    if self.stream_exhausted {
                        return None;
                    }
                    let mut bytes: usize = 0;
                    let stream = if self.materialize_left {
                        self.right_child.as_mut().unwrap()
                    } else {
                        self.left_child.as_mut().unwrap()
                    };

                    while bytes < self.stream_chunk_limit_bytes {
                        if let Some(r) = stream.next() {
                            bytes = bytes.saturating_add(Self::estimate_row_size(&r));
                            self.stream_chunk.push(r);
                        } else {
                            self.stream_exhausted = true;
                            break;
                        }
                    }
                    if self.stream_chunk.is_empty() {
                        return None;
                    }

                    let run = self.spilled_run.as_ref().unwrap();
                    self.current_reader = Some(ScratchRunReader::new(
                        self.scratch_pool_ptr,
                        run.spans.clone(),
                        run.total_bytes,
                        self.scratch_block_size,
                        false,
                    ));
                    self.current_spilled_row = self.current_reader.as_mut().unwrap().read_row();
                    self.stream_chunk_idx = 0;
                }
                if let Some(spilled) = &self.current_spilled_row {
                    if self.stream_chunk_idx < self.stream_chunk.len() {
                        let stream_row = &self.stream_chunk[self.stream_chunk_idx];
                        let res = if self.materialize_left {
                            Row::combine(spilled, stream_row)
                        } else {
                            Row::combine(stream_row, spilled)
                        };
                        self.stream_chunk_idx += 1;
                        return Some(res);
                    } else {
                        self.stream_chunk_idx = 0;
                        self.current_spilled_row = self.current_reader.as_mut().unwrap().read_row();
                    }
                } else {
                    self.stream_chunk.clear();
                }
            }
        }
    }
}