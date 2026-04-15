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
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};

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
}

impl<'a, R: Read, W: Write> CrossOperator<'a, R, W> {
    pub fn new(
        left: Box<dyn Operator + 'a>,
        right: Box<dyn Operator + 'a>,
        mat_left: bool,
        pool_ptr: *mut BufferPoolManager<R, W>,
        block_size: usize,
    ) -> Self {
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
        }
    }
}

impl<'a, R: Read, W: Write> Drop for CrossOperator<'a, R, W> {
    fn drop(&mut self) {
        if let Some(run) = &self.spilled_run {
            let pool = unsafe { &mut *self.scratch_pool_ptr };
            for &id in &run.block_ids {
                pool.disk_manager.free_anon_block(id);
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
            let mut mem_usage = 0;
            let mut writer = None;

            while let Some(row) = child.next() {
                let row_size = std::mem::size_of::<Row>()
                    + row
                        .values
                        .iter()
                        .map(|v| match v {
                            Value::String(s) => s.len(),
                            _ => 8,
                        })
                        .sum::<usize>();
                mem_usage += row_size;

                if !self.materialized_spilled && mem_usage > 10 * 1024 * 1024 {
                    self.materialized_spilled = true;
                    let mut w =
                        ScratchRunWriter::new(self.scratch_pool_ptr, self.scratch_block_size, 1024);
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
                    let mut bytes = 0;
                    let stream = if self.materialize_left {
                        self.right_child.as_mut().unwrap()
                    } else {
                        self.left_child.as_mut().unwrap()
                    };

                    while bytes < 10 * 1024 * 1024 {
                        if let Some(r) = stream.next() {
                            bytes += std::mem::size_of::<Row>() + (r.values.capacity() * 8);
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
                        run.block_ids.clone(),
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

#[derive(PartialEq)]
enum JoinState {
    Partitioning,
    LoadingBuild,
    Probing,
    Done,
}

pub struct GraceHashJoinOperator<'a, R: Read, W: Write> {
    left_child: Option<Box<dyn Operator + 'a>>,
    right_child: Option<Box<dyn Operator + 'a>>,
    left_col_idx: usize,
    right_col_idx: usize,
    build_on_left: bool,
    num_partitions: usize,
    scratch_pool_ptr: *mut BufferPoolManager<R, W>,
    scratch_block_size: usize,
    build_partitions: Vec<ScratchRun>,
    probe_partitions: Vec<ScratchRun>,
    in_memory_hash_table: FastMap<Value, Vec<usize>>,
    in_memory_build_rows: Vec<Row>,
    partition_0_spilled: bool,
    partition_0_mem_usage: usize,
    current_partition_idx: usize,
    current_build_reader: Option<ScratchRunReader<'a, R, W>>,
    current_probe_run: Option<ScratchRun>,
    current_probe_reader: Option<ScratchRunReader<'a, R, W>>,
    current_probe_row: Option<Row>,
    current_match_key: Option<Value>,
    current_match_index: usize,
    state: JoinState,
}

impl<'a, R: Read, W: Write> GraceHashJoinOperator<'a, R, W> {
    pub fn new(
        left: Box<dyn Operator + 'a>,
        right: Box<dyn Operator + 'a>,
        l_idx: usize,
        r_idx: usize,
        build_left: bool,
        parts: usize,
        pool: *mut BufferPoolManager<R, W>,
        b_size: usize,
    ) -> Self {
        Self {
            left_child: Some(left),
            right_child: Some(right),
            left_col_idx: l_idx,
            right_col_idx: r_idx,
            build_on_left: build_left,
            num_partitions: parts,
            scratch_pool_ptr: pool,
            scratch_block_size: b_size,
            build_partitions: Vec::new(),
            probe_partitions: Vec::new(),
            in_memory_hash_table: FastMap::default(),
            in_memory_build_rows: Vec::new(),
            partition_0_spilled: false,
            partition_0_mem_usage: 0,
            current_partition_idx: 0,
            current_build_reader: None,
            current_probe_run: None,
            current_probe_reader: None,
            current_probe_row: None,
            current_match_key: None,
            current_match_index: 0,
            state: JoinState::Partitioning,
        }
    }

    #[inline(always)]
    fn hash1(val: &Value) -> u64 {
        let mut s = Fnv1aHasher::default();
        val.hash(&mut s);
        s.finish()
    }

    #[inline(always)]
    fn hash2(val: &Value) -> u64 {
        let mut s = AlternateHasher::default();
        val.hash(&mut s);
        s.finish()
    }
}

impl<'a, R: Read, W: Write> Drop for GraceHashJoinOperator<'a, R, W> {
    fn drop(&mut self) {
        let pool = unsafe { &mut *self.scratch_pool_ptr };
        for run in &self.build_partitions {
            for &id in &run.block_ids {
                pool.disk_manager.free_anon_block(id);
            }
        }
        for run in &self.probe_partitions {
            for &id in &run.block_ids {
                pool.disk_manager.free_anon_block(id);
            }
        }
        if let Some(run) = &self.current_probe_run {
            for &id in &run.block_ids {
                pool.disk_manager.free_anon_block(id);
            }
        }
    }
}

impl<'a, R: Read, W: Write> Operator for GraceHashJoinOperator<'a, R, W> {
    fn next(&mut self) -> Option<Row> {
        loop {
            match self.state {
                JoinState::Partitioning => {
                    let mut build_writer = SharedBufferManager::new(
                        self.scratch_pool_ptr,
                        self.num_partitions,
                        self.scratch_block_size,
                    );
                    let mut build_child = if self.build_on_left {
                        self.left_child.take()
                    } else {
                        self.right_child.take()
                    }
                    .unwrap();
                    let build_idx = if self.build_on_left {
                        self.left_col_idx
                    } else {
                        self.right_col_idx
                    };

                    let mut bloom = BloomFilter::new(2 * 1024 * 1024);

                    while let Some(row) = build_child.next() {
                        let key = &row.values[build_idx];
                        let h1 = Self::hash1(key);
                        let h2 = Self::hash2(key);
                        bloom.insert(h1, h2);
                        let p_idx = (h1 as usize) % self.num_partitions;

                        if p_idx == 0 && !self.partition_0_spilled {
                            let key_clone = key.clone();
                            let r_idx = self.in_memory_build_rows.len();

                            let size = 64
                                + if let Value::String(s) = &row.values[build_idx] {
                                    s.len()
                                } else {
                                    0
                                };

                            self.partition_0_mem_usage += size;
                            self.in_memory_build_rows.push(row);
                            self.in_memory_hash_table
                                .entry(key_clone)
                                .or_insert_with(Vec::new)
                                .push(r_idx);

                            if self.partition_0_mem_usage > 10 * 1024 * 1024 {
                                for r in &self.in_memory_build_rows {
                                    build_writer.write_row(0, r);
                                }
                                self.in_memory_build_rows.clear();
                                self.in_memory_hash_table.clear();
                                self.partition_0_spilled = true;
                            }
                        } else {
                            build_writer.write_row(p_idx, &row);
                        }
                    }
                    self.build_partitions = build_writer.finish();

                    let mut probe_writer = SharedBufferManager::new(
                        self.scratch_pool_ptr,
                        self.num_partitions,
                        self.scratch_block_size,
                    );
                    let mut probe_child = if self.build_on_left {
                        self.right_child.take()
                    } else {
                        self.left_child.take()
                    }
                    .unwrap();
                    let probe_idx = if self.build_on_left {
                        self.right_col_idx
                    } else {
                        self.left_col_idx
                    };

                    while let Some(row) = probe_child.next() {
                        let key = &row.values[probe_idx];
                        let h1 = Self::hash1(key);
                        let h2 = Self::hash2(key);
                        if bloom.contains(h1, h2) {
                            let p_idx = (h1 as usize) % self.num_partitions;
                            probe_writer.write_row(p_idx, &row);
                        }
                    }
                    self.probe_partitions = probe_writer.finish();
                    self.state = JoinState::LoadingBuild;
                }

                JoinState::LoadingBuild => {
                    if self.current_build_reader.is_none() {
                        if self.build_partitions.is_empty()
                            && self.current_partition_idx >= self.num_partitions
                        {
                            self.state = JoinState::Done;
                            return None;
                        }
                        let build_run = self.build_partitions.remove(0);
                        self.current_probe_run = Some(self.probe_partitions.remove(0));
                        if self.current_partition_idx == 0 && !self.partition_0_spilled {
                            let probe_run = self.current_probe_run.as_ref().unwrap();
                            self.current_probe_reader = Some(ScratchRunReader::new(
                                self.scratch_pool_ptr,
                                probe_run.block_ids.clone(),
                                probe_run.total_bytes,
                                self.scratch_block_size,
                                false,
                            ));
                            self.current_probe_row =
                                self.current_probe_reader.as_mut().unwrap().read_row();
                            self.state = JoinState::Probing;
                            continue;
                        }
                        self.in_memory_hash_table.clear();
                        self.in_memory_build_rows.clear();
                        self.current_build_reader = Some(ScratchRunReader::new(
                            self.scratch_pool_ptr,
                            build_run.block_ids,
                            build_run.total_bytes,
                            self.scratch_block_size,
                            true,
                        ));
                    }

                    let mut current_memory = 0;
                    let build_idx = if self.build_on_left {
                        self.left_col_idx
                    } else {
                        self.right_col_idx
                    };
                    let build_reader = self.current_build_reader.as_mut().unwrap();

                    while let Some(row) = build_reader.read_row() {
                        let key = row.values[build_idx].clone();
                        let row_idx = self.in_memory_build_rows.len();

                        let size = 64
                            + if let Value::String(s) = &row.values[build_idx] {
                                s.len()
                            } else {
                                0
                            };

                        current_memory += size;
                        self.in_memory_build_rows.push(row);
                        self.in_memory_hash_table
                            .entry(key)
                            .or_insert_with(Vec::new)
                            .push(row_idx);

                        if current_memory > 10 * 1024 * 1024 {
                            break;
                        }
                    }

                    let probe_run = self.current_probe_run.as_ref().unwrap();
                    self.current_probe_reader = Some(ScratchRunReader::new(
                        self.scratch_pool_ptr,
                        probe_run.block_ids.clone(),
                        probe_run.total_bytes,
                        self.scratch_block_size,
                        false,
                    ));
                    self.current_probe_row = self.current_probe_reader.as_mut().unwrap().read_row();
                    self.state = JoinState::Probing;
                }

                JoinState::Probing => {
                    if self.current_probe_row.is_none() {
                        self.current_probe_reader = None;
                        let has_more_build = self
                            .current_build_reader
                            .as_ref()
                            .map_or(false, |r| r.has_more());

                        if has_more_build {
                            self.state = JoinState::LoadingBuild;
                        } else {
                            self.current_build_reader = None;
                            if let Some(run) = self.current_probe_run.take() {
                                let pool = unsafe { &mut *self.scratch_pool_ptr };
                                for &id in &run.block_ids {
                                    pool.disk_manager.free_anon_block(id);
                                }
                            }
                            self.current_partition_idx += 1;
                            self.state = JoinState::LoadingBuild;
                        }
                        continue;
                    }
                    let probe_row = self.current_probe_row.as_ref().unwrap();
                    if let Some(key) = self.current_match_key.as_ref() {
                        if let Some(build_rows) = self.in_memory_hash_table.get(key) {
                            if self.current_match_index < build_rows.len() {
                                let b_row = &self.in_memory_build_rows
                                    [build_rows[self.current_match_index]];
                                self.current_match_index += 1;
                                let (l, r) = if self.build_on_left {
                                    (b_row, probe_row)
                                } else {
                                    (probe_row, b_row)
                                };
                                return Some(Row::combine(l, r));
                            }
                        }
                        self.current_probe_row =
                            self.current_probe_reader.as_mut().unwrap().read_row();
                        self.current_match_key = None;
                        self.current_match_index = 0;
                        continue;
                    }

                    let probe_idx = if self.build_on_left {
                        self.right_col_idx
                    } else {
                        self.left_col_idx
                    };
                    let key = probe_row.values[probe_idx].clone();
                    if self.in_memory_hash_table.contains_key(&key) {
                        self.current_match_key = Some(key);
                        self.current_match_index = 0;
                    } else {
                        self.current_probe_row =
                            self.current_probe_reader.as_mut().unwrap().read_row();
                    }
                }
                JoinState::Done => {
                    return None;
                }
            }
        }
    }
}
