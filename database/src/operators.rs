use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::io::{Read, Write};
use crate::data::{Row, Value};
use crate::buffer_pool::BufferPoolManager;
use common::query::{Predicate, ComparisionValue, ComparisionOperator};
use db_config::table::TableSpec;
use std::hash::{BuildHasherDefault, Hasher, Hash};

pub struct Fnv1aHasher(u64);

impl Default for Fnv1aHasher {
    fn default() -> Self { Self(0xcbf29ce484222325) }
}

impl Hasher for Fnv1aHasher {
    fn finish(&self) -> u64 { self.0 }
    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.0 ^= byte as u64;
            self.0 = self.0.wrapping_mul(0x100000001b3);
        }
    }
}

pub type FastMap<K, V> = HashMap<K, V, BuildHasherDefault<Fnv1aHasher>>;

struct ScratchRun {
    block_ids: Vec<u64>,
    total_bytes: usize,
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
    fn new(pool_ptr: *mut BufferPoolManager<R, W>, block_size: usize, capacity_blocks: usize) -> Self {
        Self { 
            pool_ptr, block_size, 
            current_blocks: vec![0; block_size * capacity_blocks], 
            offset: 0, block_ids: Vec::new(), total_bytes: 0, 
            _marker: std::marker::PhantomData 
        }
    }

    fn write_row(&mut self, row: &Row) {
        let encoded = row.encode();
        let row_len = u32::try_from(encoded.len()).expect("External Sort: Row too large to spill");
        self.write_bytes(&row_len.to_le_bytes());
        self.write_bytes(&encoded);
    }

    fn write_bytes(&mut self, mut bytes: &[u8]) {
        self.total_bytes += bytes.len();
        while !bytes.is_empty() {
            if self.offset == self.current_blocks.len() { self.flush_buffer(); }
            let writable = (self.current_blocks.len() - self.offset).min(bytes.len());
            self.current_blocks[self.offset..self.offset + writable].copy_from_slice(&bytes[..writable]);
            self.offset += writable;
            bytes = &bytes[writable..];
        }
    }

    fn flush_buffer(&mut self) {
        if self.offset == 0 { return; }
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
        ScratchRun { block_ids: self.block_ids, total_bytes: self.total_bytes }
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
    fn new(pool_ptr: *mut BufferPoolManager<R, W>, block_ids: Vec<u64>, total_bytes: usize, block_size: usize, free_on_drop: bool) -> Self {
        Self { pool_ptr, block_ids, total_bytes, bytes_read: 0, block_idx: 0, current_block: vec![0; block_size], offset: 0, loaded: false, free_on_drop, _marker: std::marker::PhantomData }
    }

    pub fn has_more(&self) -> bool {
        self.bytes_read < self.total_bytes
    }

    fn read_row(&mut self) -> Option<Row> {
        if self.bytes_read >= self.total_bytes { return None; }
        let len_bytes = self.read_exact(4)?;
        let row_len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
        let row_bytes = self.read_exact(row_len)?;
        Row::decode(&row_bytes)
    }

    fn read_exact(&mut self, len: usize) -> Option<Vec<u8>> {
        if self.bytes_read + len > self.total_bytes { return None; }
        let mut out = vec![0; len];
        let mut written = 0;

        while written < len {
            self.ensure_block_loaded()?;
            let available = self.current_block.len() - self.offset;
            let to_copy = (len - written).min(available);
            out[written..written + to_copy].copy_from_slice(&self.current_block[self.offset..self.offset + to_copy]);
            self.offset += to_copy;
            self.bytes_read += to_copy;
            written += to_copy;
        }
        Some(out)
    }

    fn ensure_block_loaded(&mut self) -> Option<()> {
        if !self.loaded || self.offset == self.current_block.len() {
            let start_block_id = *self.block_ids.get(self.block_idx)?;

            // Cap reads at 4 blocks (16 KB) to completely prevent OOM 
            let mut num_blocks = 1;
            while self.block_idx + num_blocks < self.block_ids.len() && num_blocks < 4 {
                if self.block_ids[self.block_idx + num_blocks] == start_block_id + (num_blocks as u64) {
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
        Self { row, run_idx, sort_indices: sort_indices.to_vec() }
    }

    fn compare_rows(sort_indices: &[(usize, bool)], a: &Row, b: &Row) -> Ordering {
        for &(idx, ascending) in sort_indices {
            let cmp = a.values[idx].partial_cmp(&b.values[idx]).unwrap_or(Ordering::Equal);
            if cmp != Ordering::Equal {
                return if ascending { cmp } else { cmp.reverse() };
            }
        }
        Ordering::Equal
    }
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.run_idx == other.run_idx && Self::compare_rows(&self.sort_indices, &self.row, &other.row) == Ordering::Equal
    }
}
impl Eq for HeapEntry {}
impl PartialOrd for HeapEntry { fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) } }
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        match Self::compare_rows(&self.sort_indices, &self.row, &other.row) {
            Ordering::Less => Ordering::Greater,
            Ordering::Greater => Ordering::Less,
            Ordering::Equal => other.run_idx.cmp(&self.run_idx),
        }
    }
}

pub trait Operator { fn next(&mut self) -> Option<Row>; }

//Scan
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
    pub fn new(table_id: &str, table_spec: &'a TableSpec, buffer_pool: &'a mut BufferPoolManager<R, W>) -> Self {
        let start_block = buffer_pool.disk_manager.get_file_start_block(table_id);
        let num_blocks = buffer_pool.disk_manager.get_file_num_blocks(table_id);
        Self { 
            table_spec, buffer_pool, current_block_id: start_block, end_block_id: start_block + num_blocks, 
            current_rows: Vec::new().into_iter(),
            prefetch_buffer: Vec::new(), prefetch_offset: 0, blocks_in_buffer: 0
        }
    }
}

impl<'a, R: Read, W: Write> Operator for ScanOperator<'a, R, W> {
    fn next(&mut self) -> Option<Row> {
        loop {
            if let Some(row) = self.current_rows.next() { return Some(row); }

            if self.prefetch_offset < self.blocks_in_buffer {
                let block_size = self.buffer_pool.disk_manager.block_size;
                let start = self.prefetch_offset * block_size;
                let data = &self.prefetch_buffer[start..start + block_size];
                let rows = crate::data::deserialize_block(data, &self.table_spec.column_specs);
                self.current_rows = rows.into_iter();
                self.prefetch_offset += 1;
                continue;
            }

            if self.current_block_id < self.end_block_id {
                let max_blocks = 64; 
                let blocks_to_read = (self.end_block_id - self.current_block_id).min(max_blocks) as usize;
                
                self.prefetch_buffer = self.buffer_pool.read_pages_direct(self.current_block_id, blocks_to_read);
                self.blocks_in_buffer = blocks_to_read;
                self.prefetch_offset = 0;
                self.current_block_id += blocks_to_read as u64;
            } else {
                return None;
            }
        }
    }
}

//Filter
pub struct FilterOperator<'a> {
    child: Box<dyn Operator + 'a>,
    predicates: Vec<CompiledPredicate>,
}

enum CompiledValue {
    Column(usize), I32(i32), I64(i64), F32(f32), F64(f64), String(String),
}

struct CompiledPredicate {
    lhs_idx: usize,
    operator: ComparisionOperator,
    rhs: CompiledValue,
}

impl<'a> FilterOperator<'a> {
    pub fn new(child: Box<dyn Operator + 'a>, predicates: Vec<Predicate>, schema_map: HashMap<String, usize>) -> Self {
        let predicates = predicates.into_iter().map(|pred| CompiledPredicate {
            lhs_idx: *schema_map.get(&pred.column_name).expect("Column not found"),
            operator: pred.operator,
            rhs: match pred.value {
                ComparisionValue::I32(v) => CompiledValue::I32(v),
                ComparisionValue::I64(v) => CompiledValue::I64(v),
                ComparisionValue::F32(v) => CompiledValue::F32(v),
                ComparisionValue::F64(v) => CompiledValue::F64(v),
                ComparisionValue::String(v) => CompiledValue::String(v),
                ComparisionValue::Column(col_name) => CompiledValue::Column(*schema_map.get(&col_name).unwrap()),
            },
        }).collect();
        Self { child, predicates }
    }
}

impl<'a> Operator for FilterOperator<'a> {
    fn next(&mut self) -> Option<Row> {
        loop {
            let row = self.child.next()?;
            let mut passes = true;
            for pred in &self.predicates {
                if !evaluate_predicate(&row, pred) { passes = false; break; }
            }
            if passes { return Some(row); }
        }
    }
}

fn compare_values(lhs_val: &Value, rhs_val: &Value, operator: &ComparisionOperator) -> bool {
    if std::mem::discriminant(lhs_val) == std::mem::discriminant(rhs_val) {
        return match operator {
            ComparisionOperator::EQ => lhs_val == rhs_val,
            ComparisionOperator::NE => lhs_val != rhs_val,
            ComparisionOperator::GT => lhs_val > rhs_val,
            ComparisionOperator::LT => lhs_val < rhs_val,
            ComparisionOperator::GTE => lhs_val >= rhs_val,
            ComparisionOperator::LTE => lhs_val <= rhs_val,
        };
    }

    let get_num = |v: &Value| -> Option<f64> {
        match v {
            Value::Int32(n) => Some(*n as f64), Value::Int64(n) => Some(*n as f64),
            Value::Float32(n) => Some(*n as f64), Value::Float64(n) => Some(*n),
            _ => None,
        }
    };

    if let (Some(l), Some(r)) = (get_num(lhs_val), get_num(rhs_val)) {
        return match operator {
            ComparisionOperator::EQ => l == r, ComparisionOperator::NE => l != r,
            ComparisionOperator::GT => l > r, ComparisionOperator::LT => l < r,
            ComparisionOperator::GTE => l >= r, ComparisionOperator::LTE => l <= r,
        };
    }
    false
}

fn evaluate_predicate(row: &Row, predicate: &CompiledPredicate) -> bool {
    let lhs_val = &row.values[predicate.lhs_idx];

    match &predicate.rhs {
        CompiledValue::Column(rhs_idx) => compare_values(lhs_val, &row.values[*rhs_idx], &predicate.operator),
        CompiledValue::I32(v) => compare_values(lhs_val, &Value::Int32(*v), &predicate.operator),
        CompiledValue::I64(v) => compare_values(lhs_val, &Value::Int64(*v), &predicate.operator),
        CompiledValue::F32(v) => compare_values(lhs_val, &Value::Float32(*v), &predicate.operator),
        CompiledValue::F64(v) => compare_values(lhs_val, &Value::Float64(*v), &predicate.operator),
        CompiledValue::String(v) => {
            if let Value::String(arc_str) = lhs_val {
                let lhs_str: &str = arc_str;
                let rhs_str: &str = v;
                match predicate.operator {
                    ComparisionOperator::EQ => lhs_str == rhs_str,
                    ComparisionOperator::NE => lhs_str != rhs_str,
                    ComparisionOperator::GT => lhs_str > rhs_str,
                    ComparisionOperator::LT => lhs_str < rhs_str,
                    ComparisionOperator::GTE => lhs_str >= rhs_str,
                    ComparisionOperator::LTE => lhs_str <= rhs_str,
                }
            } else {
                false
            }
        },
    }
}

//Projection
pub struct ProjectOperator<'a> {
    child: Box<dyn Operator + 'a>,
    column_indices: Vec<usize>,
}

impl<'a> ProjectOperator<'a> {
    pub fn new(child: Box<dyn Operator + 'a>, column_indices: Vec<usize>) -> Self {
        Self { child, column_indices }
    }
}

impl<'a> Operator for ProjectOperator<'a> {
    fn next(&mut self) -> Option<Row> {
        let row = self.child.next()?;
        let mut values = Vec::with_capacity(self.column_indices.len());
        for &idx in &self.column_indices { values.push(row.values[idx].clone()); }
        Some(Row { values })
    }
}

//Sort Operator
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
    pub fn new(child: Box<dyn Operator + 'a>, sort_indices: Vec<(usize, bool)>, memory_limit_bytes: usize, scratch_pool_ptr: *mut BufferPoolManager<R, W>, scratch_block_size: usize) -> Self {
        Self { child, sort_indices, memory_limit_bytes, initialized: false, in_memory_rows: Vec::new().into_iter(), is_external: false, scratch_pool_ptr, scratch_block_size, runs: Vec::new(), run_readers: Vec::new(), merge_heap: BinaryHeap::new() }
    }

    fn estimate_row_size(row: &Row) -> usize {
        let mut size = std::mem::size_of::<Row>() + (row.values.capacity() * std::mem::size_of::<Value>());
        for value in &row.values {
            if let Value::String(s) = value {
                size += s.len(); 
            }
        }
        size
    }

    fn merge_fan_in(&self) -> usize {
        128
    }

    fn merge_runs_batch(&self, runs: Vec<ScratchRun>) -> ScratchRun {
        let mut readers = Vec::with_capacity(runs.len());
        let mut heap = BinaryHeap::new();
        for (run_idx, run) in runs.into_iter().enumerate() {
            let mut reader = ScratchRunReader::new(self.scratch_pool_ptr, run.block_ids, run.total_bytes, self.scratch_block_size, true);
            if let Some(row) = reader.read_row() { heap.push(HeapEntry::new(row, run_idx, &self.sort_indices)); }
            readers.push(reader);
        }
        let mut writer = ScratchRunWriter::new(self.scratch_pool_ptr, self.scratch_block_size, 16);
        while let Some(mut top_entry) = heap.peek_mut() {
            writer.write_row(&top_entry.row);
            if let Some(next_row) = readers[top_entry.run_idx].read_row() {
                top_entry.row = next_row;
            } else {
                std::collections::binary_heap::PeekMut::pop(top_entry);
            }
        }
        writer.finish()
    }

    fn collapse_runs(&mut self) {
        let fan_in = self.merge_fan_in();
        while self.runs.len() > fan_in {
            let mut merged_runs = Vec::new();
            let pending_runs = std::mem::take(&mut self.runs);
            let mut iter = pending_runs.into_iter();
            loop {
                let chunk: Vec<_> = iter.by_ref().take(fan_in).collect();
                if chunk.is_empty() { break; }
                if chunk.len() == 1 { merged_runs.push(chunk.into_iter().next().unwrap()); } 
                else { merged_runs.push(self.merge_runs_batch(chunk)); }
            }
            self.runs = merged_runs;
        }
    }
}

impl<'a, R: Read, W: Write> Operator for SortOperator<'a, R, W> {
    fn next(&mut self) -> Option<Row> {
        if !self.initialized {
            let mut current_run = Vec::new();
            let mut current_memory = 0;

            while let Some(row) = self.child.next() {
                current_memory += Self::estimate_row_size(&row);
                current_run.push(row);

                if current_memory >= self.memory_limit_bytes {
                    self.is_external = true;
                    current_run.sort_by(|a, b| HeapEntry::compare_rows(&self.sort_indices, a, b));
                    let mut writer = ScratchRunWriter::new(self.scratch_pool_ptr, self.scratch_block_size, 16);
                    for r in &current_run { writer.write_row(r); }
                    self.runs.push(writer.finish());

                    current_run = Vec::new(); 
                    current_memory = 0;
                }
            }

            if !current_run.is_empty() {
                current_run.sort_by(|a, b| HeapEntry::compare_rows(&self.sort_indices, a, b));
                if self.is_external {
                    let mut writer = ScratchRunWriter::new(self.scratch_pool_ptr, self.scratch_block_size, 16);
                    for r in &current_run { writer.write_row(r); }
                    self.runs.push(writer.finish());
                } else {
                    self.in_memory_rows = current_run.into_iter();
                }
            }

            if self.is_external {
                self.collapse_runs();
                for run in self.runs.drain(..) {
                    let mut reader = ScratchRunReader::new(self.scratch_pool_ptr, run.block_ids, run.total_bytes, self.scratch_block_size, true);
                    if let Some(row) = reader.read_row() {
                        let run_idx = self.run_readers.len();
                        self.merge_heap.push(HeapEntry::new(row, run_idx, &self.sort_indices));
                    }
                    self.run_readers.push(reader);
                }
            }
            self.initialized = true;
        }

        if !self.is_external { return self.in_memory_rows.next(); }

        if let Some(mut top_entry) = self.merge_heap.peek_mut() {
            if let Some(next_row) = self.run_readers[top_entry.run_idx].read_row() {
                let output_row = std::mem::replace(&mut top_entry.row, next_row);
                return Some(output_row);
            } else {
                let exhausted_entry = std::collections::binary_heap::PeekMut::pop(top_entry);
                return Some(exhausted_entry.row);
            }
        }
        None
    }
}

//Block Nested Loop Join 
pub struct CrossOperator<'a, R: Read, W: Write> {
    left_child: Option<Box<dyn Operator + 'a>>,
    right_child: Option<Box<dyn Operator + 'a>>,
    materialize_left: bool,
    
    scratch_pool_ptr: *mut BufferPoolManager<R, W>,
    scratch_block_size: usize,
    
    spilled_run: Option<ScratchRun>,
    current_reader: Option<ScratchRunReader<'a, R, W>>,
    
    stream_chunk: Vec<Row>,
    stream_chunk_idx: usize,
    current_spilled_row: Option<Row>,
    stream_exhausted: bool,
    
    initialized: bool,
}

impl<'a, R: Read, W: Write> CrossOperator<'a, R, W> {
    pub fn new(
        left_child: Box<dyn Operator + 'a>, right_child: Box<dyn Operator + 'a>, 
        materialize_left: bool, scratch_pool_ptr: *mut BufferPoolManager<R, W>, scratch_block_size: usize
    ) -> Self {
        Self { 
            left_child: Some(left_child), right_child: Some(right_child), 
            materialize_left, scratch_pool_ptr, scratch_block_size,
            spilled_run: None, current_reader: None, 
            stream_chunk: Vec::new(), stream_chunk_idx: 0, current_spilled_row: None, stream_exhausted: false,
            initialized: false 
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
            let mut writer = ScratchRunWriter::new(self.scratch_pool_ptr, self.scratch_block_size, 32);
            if self.materialize_left {
                let mut left = self.left_child.take().unwrap();
                while let Some(row) = left.next() { writer.write_row(&row); }
            } else {
                let mut right = self.right_child.take().unwrap();
                while let Some(row) = right.next() { writer.write_row(&row); }
            }
            self.spilled_run = Some(writer.finish());
            self.initialized = true;
        }

        if self.spilled_run.as_ref().unwrap().total_bytes == 0 { return None; }

        loop {
            if self.stream_chunk.is_empty() {
                if self.stream_exhausted { return None; }
                
                let mut chunk_bytes = 0;
                let stream = if self.materialize_left { self.right_child.as_mut().unwrap() } else { self.left_child.as_mut().unwrap() };
                
                while chunk_bytes < 2 * 1024 * 1024 {
                    if let Some(row) = stream.next() {
                        chunk_bytes += std::mem::size_of::<Row>() + (row.values.capacity() * std::mem::size_of::<Value>());
                        self.stream_chunk.push(row);
                    } else {
                        self.stream_exhausted = true;
                        break;
                    }
                }
                
                if self.stream_chunk.is_empty() { return None; }
                
                let run_ref = self.spilled_run.as_ref().unwrap();
                self.current_reader = Some(ScratchRunReader::new(self.scratch_pool_ptr, run_ref.block_ids.clone(), run_ref.total_bytes, self.scratch_block_size, false));
                self.current_spilled_row = self.current_reader.as_mut().unwrap().read_row();
                self.stream_chunk_idx = 0;
            }
            
            if let Some(spilled_row) = &self.current_spilled_row {
                if self.stream_chunk_idx < self.stream_chunk.len() {
                    let stream_row = &self.stream_chunk[self.stream_chunk_idx];
                    let result = if self.materialize_left {
                        Row::combine(spilled_row, stream_row)
                    } else {
                        Row::combine(stream_row, spilled_row)
                    };
                    self.stream_chunk_idx += 1;
                    return Some(result);
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

// Grace Hash Join
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
        left_child: Box<dyn Operator + 'a>, right_child: Box<dyn Operator + 'a>, 
        left_col_idx: usize, right_col_idx: usize, build_on_left: bool,
        num_partitions: usize, scratch_pool_ptr: *mut BufferPoolManager<R, W>, scratch_block_size: usize,
    ) -> Self {
        Self { 
            left_child: Some(left_child), right_child: Some(right_child), left_col_idx, right_col_idx, 
            build_on_left, num_partitions, scratch_pool_ptr, scratch_block_size, 
            build_partitions: Vec::new(), probe_partitions: Vec::new(), 
            in_memory_hash_table: FastMap::default(), in_memory_build_rows: Vec::new(),
            current_build_reader: None, current_probe_run: None,
            current_probe_reader: None, current_probe_row: None, current_match_key: None, 
            current_match_index: 0, state: JoinState::Partitioning 
        }
    }

    fn calculate_hash(val: &Value) -> usize {
        let mut s = Fnv1aHasher::default();
        val.hash(&mut s);
        s.finish() as usize
    }
}

impl<'a, R: Read, W: Write> Drop for GraceHashJoinOperator<'a, R, W> {
    fn drop(&mut self) {
        let pool = unsafe { &mut *self.scratch_pool_ptr };
        for run in &self.build_partitions {
            for &id in &run.block_ids { pool.disk_manager.free_anon_block(id); }
        }
        for run in &self.probe_partitions {
            for &id in &run.block_ids { pool.disk_manager.free_anon_block(id); }
        }
        if let Some(run) = &self.current_probe_run {
            for &id in &run.block_ids { pool.disk_manager.free_anon_block(id); }
        }
    }
}

impl<'a, R: Read, W: Write> Operator for GraceHashJoinOperator<'a, R, W> {
    fn next(&mut self) -> Option<Row> {
        loop {
            match self.state {
                JoinState::Partitioning => {
                    let mut build_writers = Vec::with_capacity(self.num_partitions);
                    let mut probe_writers = Vec::with_capacity(self.num_partitions);
                    for _ in 0..self.num_partitions {
                        build_writers.push(ScratchRunWriter::new(self.scratch_pool_ptr, self.scratch_block_size, 4));
                        probe_writers.push(ScratchRunWriter::new(self.scratch_pool_ptr, self.scratch_block_size, 4));
                    }
                    
                    let mut build_child = if self.build_on_left { self.left_child.take() } else { self.right_child.take() }.unwrap();
                    let build_idx = if self.build_on_left { self.left_col_idx } else { self.right_col_idx };
                    while let Some(row) = build_child.next() {
                        let p_idx = Self::calculate_hash(&row.values[build_idx]) % self.num_partitions;
                        build_writers[p_idx].write_row(&row);
                    }
                    
                    let mut probe_child = if self.build_on_left { self.right_child.take() } else { self.left_child.take() }.unwrap();
                    let probe_idx = if self.build_on_left { self.right_col_idx } else { self.left_col_idx };
                    while let Some(row) = probe_child.next() {
                        let p_idx = Self::calculate_hash(&row.values[probe_idx]) % self.num_partitions;
                        probe_writers[p_idx].write_row(&row);
                    }
                    
                    for w in build_writers { self.build_partitions.push(w.finish()); }
                    for w in probe_writers { self.probe_partitions.push(w.finish()); }
                    self.state = JoinState::LoadingBuild;
                }
                
                JoinState::LoadingBuild => {
                    if self.current_build_reader.is_none() {
                        if self.build_partitions.is_empty() {
                            self.state = JoinState::Done;
                            return None;
                        }
                        
                        let build_run = self.build_partitions.remove(0); 
                        self.current_build_reader = Some(ScratchRunReader::new(self.scratch_pool_ptr, build_run.block_ids, build_run.total_bytes, self.scratch_block_size, true));
                        self.current_probe_run = Some(self.probe_partitions.remove(0));
                    }

                    self.in_memory_hash_table.clear();
                    self.in_memory_build_rows.clear();
                    
                    let mut current_memory = 0;
                    let build_idx = if self.build_on_left { self.left_col_idx } else { self.right_col_idx };
                    
                    let build_reader = self.current_build_reader.as_mut().unwrap();

                    while let Some(row) = build_reader.read_row() {
                        let key = row.values[build_idx].clone();
                        let row_idx = self.in_memory_build_rows.len();
                        
                        let mut size = 64; 
                        for v in &row.values {
                            if let Value::String(s) = v { size += s.len(); }
                        }
                        current_memory += size;
                        
                        self.in_memory_build_rows.push(row);
                        self.in_memory_hash_table.entry(key).or_insert_with(Vec::new).push(row_idx);
                        
                        if current_memory > 12 * 1024 * 1024 {
                            break;
                        }
                    }
                    
                    let probe_run = self.current_probe_run.as_ref().unwrap();
                    
                    self.current_probe_reader = Some(ScratchRunReader::new(self.scratch_pool_ptr, probe_run.block_ids.clone(), probe_run.total_bytes, self.scratch_block_size, false));
                    self.current_probe_row = self.current_probe_reader.as_mut().unwrap().read_row();
                    self.state = JoinState::Probing;
                }
                
                JoinState::Probing => {
                    if self.current_probe_row.is_none() {
                        self.current_probe_reader = None; 
                        
                        let build_reader = self.current_build_reader.as_ref().unwrap();
                        if build_reader.has_more() {
                            self.state = JoinState::LoadingBuild;
                        } else {
                            self.current_build_reader = None; 
                            
                            if let Some(run) = self.current_probe_run.take() {
                                let pool = unsafe { &mut *self.scratch_pool_ptr };
                                for &id in &run.block_ids {
                                    pool.disk_manager.free_anon_block(id);
                                }
                            }
                            
                            self.state = JoinState::LoadingBuild;
                        }
                        continue;
                    }
                    
                    let probe_row = self.current_probe_row.as_ref().unwrap();
                    
                    if let Some(key) = self.current_match_key.as_ref() {
                        if let Some(build_rows) = self.in_memory_hash_table.get(key) {
                            if self.current_match_index < build_rows.len() {
                                let b_row = &self.in_memory_build_rows[build_rows[self.current_match_index]];
                                self.current_match_index += 1;
                                
                                let (left_row, right_row) = if self.build_on_left {
                                    (b_row, probe_row)
                                } else {
                                    (probe_row, b_row)
                                };
                                return Some(Row::combine(left_row, right_row));
                            }
                        }
                        
                        self.current_probe_row = self.current_probe_reader.as_mut().unwrap().read_row();
                        self.current_match_key = None;
                        self.current_match_index = 0;
                        continue;
                    }
                    
                    let probe_idx = if self.build_on_left { self.right_col_idx } else { self.left_col_idx };
                    let key = probe_row.values[probe_idx].clone();
                    
                    if self.in_memory_hash_table.contains_key(&key) {
                        self.current_match_key = Some(key);
                        self.current_match_index = 0;
                    } else {
                        self.current_probe_row = self.current_probe_reader.as_mut().unwrap().read_row();
                    }
                }
                
                JoinState::Done => { return None; }
            }
        }
    }
}