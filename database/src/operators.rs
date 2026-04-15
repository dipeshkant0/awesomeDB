use crate::buffer_pool::BufferPoolManager;
use crate::data::{Row, Value};
use common::query::{ComparisionOperator, ComparisionValue, Predicate};
use db_config::table::TableSpec;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::io::{Read, Write};

pub struct Fnv1aHasher(u64);
impl Default for Fnv1aHasher {
    fn default() -> Self {
        Self(0xcbf29ce484222325)
    }
}
impl Hasher for Fnv1aHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.0 ^= byte as u64;
            self.0 = self.0.wrapping_mul(0x100000001b3);
        }
    }
}

pub struct AlternateHasher(u64);
impl Default for AlternateHasher {
    fn default() -> Self {
        Self(0x100000001b3)
    }
}
impl Hasher for AlternateHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.0 ^= byte as u64;
            self.0 = self.0.wrapping_mul(0xcbf29ce484222325);
        }
    }
}

pub type FastMap<K, V> = HashMap<K, V, BuildHasherDefault<Fnv1aHasher>>;

struct BloomFilter {
    bits: Vec<u8>,
    num_bits: u64,
}
impl BloomFilter {
    fn new(size_bytes: usize) -> Self {
        Self {
            bits: vec![0; size_bytes],
            num_bits: (size_bytes as u64) * 8,
        }
    }
    fn insert(&mut self, h1: u64, h2: u64) {
        let idx1 = (h1 % self.num_bits) as usize;
        let idx2 = (h2 % self.num_bits) as usize;
        self.bits[idx1 / 8] |= 1 << (idx1 % 8);
        self.bits[idx2 / 8] |= 1 << (idx2 % 8);
    }
    fn contains(&self, h1: u64, h2: u64) -> bool {
        let idx1 = (h1 % self.num_bits) as usize;
        let idx2 = (h2 % self.num_bits) as usize;
        (self.bits[idx1 / 8] & (1 << (idx1 % 8))) != 0
            && (self.bits[idx2 / 8] & (1 << (idx2 % 8))) != 0
    }
}

struct ScratchRun {
    block_ids: Vec<u64>,
    total_bytes: usize,
}

struct SharedBufferManager<'a, R: Read, W: Write> {
    pool_ptr: *mut BufferPoolManager<R, W>,
    block_size: usize,
    partition_buffers: Vec<Vec<u8>>,
    partition_block_ids: Vec<Vec<u64>>,
    partition_byte_counts: Vec<usize>,
    total_buffered_bytes: usize,
    flush_threshold: usize,
    _marker: std::marker::PhantomData<&'a mut BufferPoolManager<R, W>>,
}

impl<'a, R: Read, W: Write> SharedBufferManager<'a, R, W> {
    fn new(
        pool_ptr: *mut BufferPoolManager<R, W>,
        num_partitions: usize,
        block_size: usize,
    ) -> Self {
        Self {
            pool_ptr,
            block_size,
            partition_buffers: vec![Vec::new(); num_partitions],
            partition_block_ids: vec![Vec::new(); num_partitions],
            partition_byte_counts: vec![0; num_partitions],
            total_buffered_bytes: 0,
            flush_threshold: 8 * 1024 * 1024,
            _marker: std::marker::PhantomData,
        }
    }
    fn write_row(&mut self, p_idx: usize, row: &Row) {
        let encoded = row.encode();
        let row_len = (encoded.len() as u32).to_le_bytes();
        self.partition_buffers[p_idx].extend_from_slice(&row_len);
        self.partition_buffers[p_idx].extend_from_slice(&encoded);
        let bytes_added = 4 + encoded.len();
        self.partition_byte_counts[p_idx] += bytes_added;
        self.total_buffered_bytes += bytes_added;
        if self.total_buffered_bytes >= self.flush_threshold {
            self.flush_largest();
        }
    }
    fn flush_largest(&mut self) {
        let (p_idx, _) = self
            .partition_buffers
            .iter()
            .enumerate()
            .max_by_key(|(_, b)| b.len())
            .unwrap();
        let buf = &mut self.partition_buffers[p_idx];
        let num_blocks = buf.len() / self.block_size;
        if num_blocks == 0 {
            return;
        }
        let pool = unsafe { &mut *self.pool_ptr };
        let start_block = pool.disk_manager.allocate_anon_blocks(num_blocks as u64);
        let flush_bytes = num_blocks * self.block_size;
        pool.write_pages_direct(start_block, num_blocks, &buf[0..flush_bytes]);
        for i in 0..num_blocks {
            self.partition_block_ids[p_idx].push(start_block + i as u64);
        }
        self.total_buffered_bytes -= flush_bytes;
        buf.drain(0..flush_bytes);
    }
    fn finish(mut self) -> Vec<ScratchRun> {
        for i in 0..self.partition_buffers.len() {
            if !self.partition_buffers[i].is_empty() {
                let pool = unsafe { &mut *self.pool_ptr };
                let buf = &mut self.partition_buffers[i];
                let num_blocks = (buf.len() + self.block_size - 1) / self.block_size;
                let start_block = pool.disk_manager.allocate_anon_blocks(num_blocks as u64);
                buf.resize(num_blocks * self.block_size, 0);
                pool.write_pages_direct(start_block, num_blocks, buf);
                for j in 0..num_blocks {
                    self.partition_block_ids[i].push(start_block + j as u64);
                }
            }
        }
        self.partition_block_ids
            .into_iter()
            .zip(self.partition_byte_counts.into_iter())
            .map(|(ids, bytes)| ScratchRun {
                block_ids: ids,
                total_bytes: bytes,
            })
            .collect()
    }
}

struct ScratchRunWriter<'a, R: Read, W: Write> {
    pool_ptr: *mut BufferPoolManager<R, W>,
    block_size: usize,
    current_blocks: Vec<u8>,
    offset: usize,
    block_ids: Vec<u64>,
    total_bytes: usize,
    _marker: std::marker::PhantomData<&'a mut BufferPoolManager<R, W>>,
}
impl<'a, R: Read, W: Write> ScratchRunWriter<'a, R, W> {
    fn new(
        pool_ptr: *mut BufferPoolManager<R, W>,
        block_size: usize,
        capacity_blocks: usize,
    ) -> Self {
        Self {
            pool_ptr,
            block_size,
            current_blocks: vec![0; block_size * capacity_blocks],
            offset: 0,
            block_ids: Vec::new(),
            total_bytes: 0,
            _marker: std::marker::PhantomData,
        }
    }
    fn write_row(&mut self, row: &Row) {
        let encoded = row.encode();
        let row_len = u32::try_from(encoded.len()).unwrap();
        self.write_bytes(&row_len.to_le_bytes());
        self.write_bytes(&encoded);
    }
    fn write_bytes(&mut self, mut bytes: &[u8]) {
        self.total_bytes += bytes.len();
        while !bytes.is_empty() {
            if self.offset == self.current_blocks.len() {
                self.flush_buffer();
            }
            let writable = (self.current_blocks.len() - self.offset).min(bytes.len());
            self.current_blocks[self.offset..self.offset + writable]
                .copy_from_slice(&bytes[..writable]);
            self.offset += writable;
            bytes = &bytes[writable..];
        }
    }
    fn flush_buffer(&mut self) {
        if self.offset == 0 {
            return;
        }
        let pool = unsafe { &mut *self.pool_ptr };
        let num_blocks = (self.offset + self.block_size - 1) / self.block_size;
        let start_block = pool.disk_manager.allocate_anon_blocks(num_blocks as u64);
        let valid_len = self.offset;
        let target_len = num_blocks * self.block_size;
        self.current_blocks[valid_len..target_len].fill(0);
        pool.write_pages_direct(start_block, num_blocks, &self.current_blocks[0..target_len]);
        for i in 0..num_blocks {
            self.block_ids.push(start_block + (i as u64));
        }
        self.offset = 0;
    }
    fn finish(mut self) -> ScratchRun {
        self.flush_buffer();
        ScratchRun {
            block_ids: self.block_ids,
            total_bytes: self.total_bytes,
        }
    }
}

struct ScratchRunReader<'a, R: Read, W: Write> {
    pool_ptr: *mut BufferPoolManager<R, W>,
    block_ids: Vec<u64>,
    total_bytes: usize,
    bytes_read: usize,
    block_idx: usize,
    current_block: Vec<u8>,
    offset: usize,
    loaded: bool,
    free_on_drop: bool,
    _marker: std::marker::PhantomData<&'a mut BufferPoolManager<R, W>>,
}
impl<'a, R: Read, W: Write> ScratchRunReader<'a, R, W> {
    fn new(
        pool_ptr: *mut BufferPoolManager<R, W>,
        block_ids: Vec<u64>,
        total_bytes: usize,
        block_size: usize,
        free_on_drop: bool,
    ) -> Self {
        Self {
            pool_ptr,
            block_ids,
            total_bytes,
            bytes_read: 0,
            block_idx: 0,
            current_block: vec![0; block_size],
            offset: 0,
            loaded: false,
            free_on_drop,
            _marker: std::marker::PhantomData,
        }
    }
    pub fn has_more(&self) -> bool {
        self.bytes_read < self.total_bytes
    }
    fn read_row(&mut self) -> Option<Row> {
        if self.bytes_read >= self.total_bytes {
            return None;
        }
        let len_bytes = self.read_exact(4)?;
        let row_len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
        let row_bytes = self.read_exact(row_len)?;
        Row::decode(&row_bytes)
    }
    fn read_exact(&mut self, len: usize) -> Option<Vec<u8>> {
        if self.bytes_read + len > self.total_bytes {
            return None;
        }
        let mut out = vec![0; len];
        let mut written = 0;
        while written < len {
            self.ensure_block_loaded()?;
            let available = self.current_block.len() - self.offset;
            let to_copy = (len - written).min(available);
            out[written..written + to_copy]
                .copy_from_slice(&self.current_block[self.offset..self.offset + to_copy]);
            self.offset += to_copy;
            self.bytes_read += to_copy;
            written += to_copy;
        }
        Some(out)
    }
    fn ensure_block_loaded(&mut self) -> Option<()> {
        if !self.loaded || self.offset == self.current_block.len() {
            let start_block_id = *self.block_ids.get(self.block_idx)?;
            let mut num_blocks = 1;
            while self.block_idx + num_blocks < self.block_ids.len() && num_blocks < 4 {
                if self.block_ids[self.block_idx + num_blocks]
                    == start_block_id + (num_blocks as u64)
                {
                    num_blocks += 1;
                } else {
                    break;
                }
            }
            let pool = unsafe { &mut *self.pool_ptr };
            self.current_block = pool.read_pages_direct(start_block_id, num_blocks);
            self.block_idx += num_blocks;
            self.offset = 0;
            self.loaded = true;
        }
        Some(())
    }
}
impl<'a, R: Read, W: Write> Drop for ScratchRunReader<'a, R, W> {
    fn drop(&mut self) {
        if self.free_on_drop {
            let pool = unsafe { &mut *self.pool_ptr };
            for &id in &self.block_ids {
                pool.disk_manager.free_anon_block(id);
            }
        }
    }
}

struct HeapEntry {
    row: Row,
    run_idx: usize,
    sort_indices: Vec<(usize, bool)>,
}
impl HeapEntry {
    fn new(row: Row, run_idx: usize, sort_indices: &[(usize, bool)]) -> Self {
        Self {
            row,
            run_idx,
            sort_indices: sort_indices.to_vec(),
        }
    }
    fn compare_rows(sort_indices: &[(usize, bool)], a: &Row, b: &Row) -> Ordering {
        for &(idx, ascending) in sort_indices {
            let cmp = a.values[idx]
                .partial_cmp(&b.values[idx])
                .unwrap_or(Ordering::Equal);
            if cmp != Ordering::Equal {
                return if ascending { cmp } else { cmp.reverse() };
            }
        }
        Ordering::Equal
    }
}
impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.run_idx == other.run_idx
            && Self::compare_rows(&self.sort_indices, &self.row, &other.row) == Ordering::Equal
    }
}
impl Eq for HeapEntry {}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        match Self::compare_rows(&self.sort_indices, &self.row, &other.row) {
            Ordering::Less => Ordering::Greater,
            Ordering::Greater => Ordering::Less,
            Ordering::Equal => other.run_idx.cmp(&self.run_idx),
        }
    }
}

pub trait Operator {
    fn next(&mut self) -> Option<Row>;
}

// SCAN
pub struct ScanOperator<'a, R: Read, W: Write> {
    table_spec: &'a TableSpec,
    buffer_pool: &'a mut BufferPoolManager<R, W>,
    current_block_id: u64,
    end_block_id: u64,
    current_rows: std::vec::IntoIter<Row>,
    prefetch_buffer: Vec<u8>,
    prefetch_offset: usize,
    blocks_in_buffer: usize,
}
impl<'a, R: Read, W: Write> ScanOperator<'a, R, W> {
    pub fn new(
        table_id: &str,
        table_spec: &'a TableSpec,
        buffer_pool: &'a mut BufferPoolManager<R, W>,
    ) -> Self {
        let start_block = buffer_pool.disk_manager.get_file_start_block(table_id);
        let num_blocks = buffer_pool.disk_manager.get_file_num_blocks(table_id);
        Self {
            table_spec,
            buffer_pool,
            current_block_id: start_block,
            end_block_id: start_block + num_blocks,
            current_rows: Vec::new().into_iter(),
            prefetch_buffer: Vec::new(),
            prefetch_offset: 0,
            blocks_in_buffer: 0,
        }
    }
}
impl<'a, R: Read, W: Write> Operator for ScanOperator<'a, R, W> {
    fn next(&mut self) -> Option<Row> {
        loop {
            if let Some(row) = self.current_rows.next() {
                return Some(row);
            }
            if self.prefetch_offset < self.blocks_in_buffer {
                let block_size = self.buffer_pool.disk_manager.block_size;
                let start = self.prefetch_offset * block_size;
                let data = &self.prefetch_buffer[start..start + block_size];
                self.current_rows =
                    crate::data::deserialize_block(data, &self.table_spec.column_specs).into_iter();
                self.prefetch_offset += 1;
                continue;
            }
            if self.current_block_id < self.end_block_id {
                let blocks_to_read = (self.end_block_id - self.current_block_id).min(64) as usize;
                self.prefetch_buffer = self
                    .buffer_pool
                    .read_pages_direct(self.current_block_id, blocks_to_read);
                self.blocks_in_buffer = blocks_to_read;
                self.prefetch_offset = 0;
                self.current_block_id += blocks_to_read as u64;
            } else {
                return None;
            }
        }
    }
}

// FILTER
pub struct FilterOperator<'a> {
    child: Box<dyn Operator + 'a>,
    predicates: Vec<CompiledPredicate>,
}
enum CompiledValue {
    Column(usize),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    String(String),
}
struct CompiledPredicate {
    lhs_idx: usize,
    operator: ComparisionOperator,
    rhs: CompiledValue,
}
impl<'a> FilterOperator<'a> {
    pub fn new(
        child: Box<dyn Operator + 'a>,
        predicates: Vec<Predicate>,
        schema_map: HashMap<String, usize>,
    ) -> Self {
        let predicates = predicates
            .into_iter()
            .map(|pred| CompiledPredicate {
                lhs_idx: *schema_map.get(&pred.column_name).unwrap(),
                operator: pred.operator,
                rhs: match pred.value {
                    ComparisionValue::I32(v) => CompiledValue::I32(v),
                    ComparisionValue::I64(v) => CompiledValue::I64(v),
                    ComparisionValue::F32(v) => CompiledValue::F32(v),
                    ComparisionValue::F64(v) => CompiledValue::F64(v),
                    ComparisionValue::String(v) => CompiledValue::String(v),
                    ComparisionValue::Column(c) => {
                        CompiledValue::Column(*schema_map.get(&c).unwrap())
                    }
                },
            })
            .collect();
        Self { child, predicates }
    }
}
impl<'a> Operator for FilterOperator<'a> {
    fn next(&mut self) -> Option<Row> {
        loop {
            let row = self.child.next()?;
            if self
                .predicates
                .iter()
                .all(|pred| evaluate_predicate(&row, pred))
            {
                return Some(row);
            }
        }
    }
}

fn compare_values(lhs: &Value, rhs: &Value, op: &ComparisionOperator) -> bool {
    if std::mem::discriminant(lhs) == std::mem::discriminant(rhs) {
        return match op {
            ComparisionOperator::EQ => lhs == rhs,
            ComparisionOperator::NE => lhs != rhs,
            ComparisionOperator::GT => lhs > rhs,
            ComparisionOperator::LT => lhs < rhs,
            ComparisionOperator::GTE => lhs >= rhs,
            ComparisionOperator::LTE => lhs <= rhs,
        };
    }
    let get_num = |v: &Value| -> Option<f64> {
        match v {
            Value::Int32(n) => Some(*n as f64),
            Value::Int64(n) => Some(*n as f64),
            Value::Float32(n) => Some(*n as f64),
            Value::Float64(n) => Some(*n),
            _ => None,
        }
    };
    if let (Some(l), Some(r)) = (get_num(lhs), get_num(rhs)) {
        return match op {
            ComparisionOperator::EQ => l == r,
            ComparisionOperator::NE => l != r,
            ComparisionOperator::GT => l > r,
            ComparisionOperator::LT => l < r,
            ComparisionOperator::GTE => l >= r,
            ComparisionOperator::LTE => l <= r,
        };
    }
    false
}
fn evaluate_predicate(row: &Row, predicate: &CompiledPredicate) -> bool {
    let lhs_val = &row.values[predicate.lhs_idx];
    match &predicate.rhs {
        CompiledValue::Column(r) => compare_values(lhs_val, &row.values[*r], &predicate.operator),
        CompiledValue::I32(v) => compare_values(lhs_val, &Value::Int32(*v), &predicate.operator),
        CompiledValue::I64(v) => compare_values(lhs_val, &Value::Int64(*v), &predicate.operator),
        CompiledValue::F32(v) => compare_values(lhs_val, &Value::Float32(*v), &predicate.operator),
        CompiledValue::F64(v) => compare_values(lhs_val, &Value::Float64(*v), &predicate.operator),
        CompiledValue::String(v) => {
            if let Value::String(s) = lhs_val {
                match predicate.operator {
                    ComparisionOperator::EQ => s.as_ref() == v,
                    ComparisionOperator::NE => s.as_ref() != v,
                    ComparisionOperator::GT => s.as_ref() > v,
                    ComparisionOperator::LT => s.as_ref() < v,
                    ComparisionOperator::GTE => s.as_ref() >= v,
                    ComparisionOperator::LTE => s.as_ref() <= v,
                }
            } else {
                false
            }
        }
    }
}

pub enum ProjectSource {
    Index(usize),
    Literal(String),
}

pub struct ProjectOperator<'a> {
    child: Box<dyn Operator + 'a>,
    sources: Vec<ProjectSource>,
}
impl<'a> ProjectOperator<'a> {
    pub fn new(child: Box<dyn Operator + 'a>, sources: Vec<ProjectSource>) -> Self {
        Self { child, sources }
    }
}
impl<'a> Operator for ProjectOperator<'a> {
    fn next(&mut self) -> Option<Row> {
        let row = self.child.next()?;
        let values = self
            .sources
            .iter()
            .map(|src| match src {
                ProjectSource::Index(idx) => row.values[*idx].clone(),
                ProjectSource::Literal(s) => Value::String(std::sync::Arc::from(s.as_str())),
            })
            .collect();
        Some(Row { values })
    }
}

// SORT
pub struct SortOperator<'a, R: Read, W: Write> {
    child: Box<dyn Operator + 'a>,
    sort_indices: Vec<(usize, bool)>,
    memory_limit_bytes: usize,
    initialized: bool,
    in_memory_rows: std::vec::IntoIter<Row>,
    is_external: bool,
    scratch_pool_ptr: *mut BufferPoolManager<R, W>,
    scratch_block_size: usize,
    runs: Vec<ScratchRun>,
    run_readers: Vec<ScratchRunReader<'a, R, W>>,
    merge_heap: BinaryHeap<HeapEntry>,
}
impl<'a, R: Read, W: Write> SortOperator<'a, R, W> {
    pub fn new(
        child: Box<dyn Operator + 'a>,
        sort_indices: Vec<(usize, bool)>,
        memory_limit_bytes: usize,
        scratch_pool_ptr: *mut BufferPoolManager<R, W>,
        scratch_block_size: usize,
    ) -> Self {
        Self {
            child,
            sort_indices,
            memory_limit_bytes,
            initialized: false,
            in_memory_rows: Vec::new().into_iter(),
            is_external: false,
            scratch_pool_ptr,
            scratch_block_size,
            runs: Vec::new(),
            run_readers: Vec::new(),
            merge_heap: BinaryHeap::new(),
        }
    }
    fn estimate_row_size(row: &Row) -> usize {
        std::mem::size_of::<Row>()
            + row
                .values
                .iter()
                .map(|v| match v {
                    Value::String(s) => std::mem::size_of::<Value>() + s.len(),
                    _ => std::mem::size_of::<Value>(),
                })
                .sum::<usize>()
    }
    fn merge_fan_in(&self) -> usize {
        128
    }
    fn merge_runs_batch(&self, runs: Vec<ScratchRun>) -> ScratchRun {
        let mut readers = Vec::with_capacity(runs.len());
        let mut heap = BinaryHeap::new();
        for (idx, run) in runs.into_iter().enumerate() {
            let mut reader = ScratchRunReader::new(
                self.scratch_pool_ptr,
                run.block_ids,
                run.total_bytes,
                self.scratch_block_size,
                true,
            );
            if let Some(row) = reader.read_row() {
                heap.push(HeapEntry::new(row, idx, &self.sort_indices));
            }
            readers.push(reader);
        }
        let mut writer = ScratchRunWriter::new(self.scratch_pool_ptr, self.scratch_block_size, 16);
        while let Some(mut top) = heap.peek_mut() {
            writer.write_row(&top.row);
            if let Some(next) = readers[top.run_idx].read_row() {
                top.row = next;
            } else {
                std::collections::binary_heap::PeekMut::pop(top);
            }
        }
        writer.finish()
    }
    fn collapse_runs(&mut self) {
        let fan_in = self.merge_fan_in();
        while self.runs.len() > fan_in {
            let pending = std::mem::take(&mut self.runs);
            let mut iter = pending.into_iter();
            loop {
                let chunk: Vec<_> = iter.by_ref().take(fan_in).collect();
                if chunk.is_empty() {
                    break;
                }
                if chunk.len() == 1 {
                    self.runs.push(chunk.into_iter().next().unwrap());
                } else {
                    self.runs.push(self.merge_runs_batch(chunk));
                }
            }
        }
    }
}
impl<'a, R: Read, W: Write> Operator for SortOperator<'a, R, W> {
    fn next(&mut self) -> Option<Row> {
        if !self.initialized {
            let mut run = Vec::new();
            let mut mem = 0;
            while let Some(row) = self.child.next() {
                mem += Self::estimate_row_size(&row);
                run.push(row);
                if mem >= self.memory_limit_bytes {
                    self.is_external = true;
                    run.sort_by(|a, b| HeapEntry::compare_rows(&self.sort_indices, a, b));
                    let mut writer =
                        ScratchRunWriter::new(self.scratch_pool_ptr, self.scratch_block_size, 16);
                    for r in &run {
                        writer.write_row(r);
                    }
                    self.runs.push(writer.finish());
                    run = Vec::new(); // MEMORY FIX: Fully return backing block to allocator
                    mem = 0;
                }
            }
            if !run.is_empty() {
                run.sort_by(|a, b| HeapEntry::compare_rows(&self.sort_indices, a, b));
                if self.is_external {
                    let mut writer =
                        ScratchRunWriter::new(self.scratch_pool_ptr, self.scratch_block_size, 16);
                    for r in &run {
                        writer.write_row(r);
                    }
                    self.runs.push(writer.finish());
                } else {
                    self.in_memory_rows = run.into_iter();
                }
            }
            if self.is_external {
                self.collapse_runs();
                for run in self.runs.drain(..) {
                    let mut reader = ScratchRunReader::new(
                        self.scratch_pool_ptr,
                        run.block_ids,
                        run.total_bytes,
                        self.scratch_block_size,
                        true,
                    );
                    if let Some(r) = reader.read_row() {
                        self.merge_heap.push(HeapEntry::new(
                            r,
                            self.run_readers.len(),
                            &self.sort_indices,
                        ));
                    }
                    self.run_readers.push(reader);
                }
            }
            self.initialized = true;
        }
        if !self.is_external {
            return self.in_memory_rows.next();
        }
        if let Some(mut top) = self.merge_heap.peek_mut() {
            if let Some(next) = self.run_readers[top.run_idx].read_row() {
                return Some(std::mem::replace(&mut top.row, next));
            } else {
                return Some(std::collections::binary_heap::PeekMut::pop(top).row);
            }
        }
        None
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

                // OOM PROTECTION: 10 MB strict limit to prevent Vec capacity-doubling from breaching 64MB OS limit
                if !self.materialized_spilled && mem_usage > 10 * 1024 * 1024 {
                    self.materialized_spilled = true;
                    let mut w =
                        ScratchRunWriter::new(self.scratch_pool_ptr, self.scratch_block_size, 32);
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

                    // OOM PROTECTION: 10 MB strict streaming block
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

// HYBRID GRACE HASH JOIN
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
    fn hash1(val: &Value) -> u64 {
        let mut s = Fnv1aHasher::default();
        val.hash(&mut s);
        s.finish()
    }
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
                            let mut size = 64;
                            for v in &row.values {
                                if let Value::String(s) = v {
                                    size += s.len();
                                }
                            }
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
                                self.in_memory_build_rows = Vec::new();
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
                        let mut size = 64;
                        for v in &row.values {
                            if let Value::String(s) = v {
                                size += s.len();
                            }
                        }
                        current_memory += size;
                        self.in_memory_build_rows.push(row);
                        self.in_memory_hash_table
                            .entry(key)
                            .or_insert_with(Vec::new)
                            .push(row_idx);
                        if current_memory > 10 * 1024 * 1024 {
                            break;
                        } // Safe 10MB chunk size
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
                        let has_more_build = if let Some(reader) = &self.current_build_reader {
                            reader.has_more()
                        } else {
                            false
                        };
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
