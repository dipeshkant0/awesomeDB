use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::io::{Read, Write};
use crate::data::{Row, Value};
use crate::buffer_pool::BufferPoolManager;
use common::query::{Predicate, ComparisionValue, ComparisionOperator};
use db_config::table::TableSpec;

struct ScratchRun {
    block_ids: Vec<u64>,
    total_bytes: usize,
}

struct ScratchRunWriter<'a, R: Read, W: Write> {
    pool_ptr: *mut BufferPoolManager<R, W>,
    block_size: usize,
    current_block: Vec<u8>,
    offset: usize,
    block_ids: Vec<u64>,
    total_bytes: usize,
    _marker: std::marker::PhantomData<&'a mut BufferPoolManager<R, W>>,
}

impl<'a, R: Read, W: Write> ScratchRunWriter<'a, R, W> {
    fn new(pool_ptr: *mut BufferPoolManager<R, W>, block_size: usize) -> Self {
        Self { pool_ptr, block_size, current_block: vec![0; block_size], offset: 0, block_ids: Vec::new(), total_bytes: 0, _marker: std::marker::PhantomData }
    }

    fn write_row(&mut self, row: &Row) {
        let encoded = row.encode();
        let row_len = u32::try_from(encoded.len()).expect("External Sort: Row too large to spill");
        self.write_bytes(&row_len.to_le_bytes());
        self.write_bytes(&encoded);
    }

    fn finish(mut self) -> ScratchRun {
        if self.offset > 0 { self.flush_block(); }
        ScratchRun { block_ids: self.block_ids, total_bytes: self.total_bytes }
    }

    fn write_bytes(&mut self, mut bytes: &[u8]) {
        self.total_bytes += bytes.len();
        while !bytes.is_empty() {
            if self.offset == self.block_size { self.flush_block(); }
            let writable = (self.block_size - self.offset).min(bytes.len());
            self.current_block[self.offset..self.offset + writable].copy_from_slice(&bytes[..writable]);
            self.offset += writable;
            bytes = &bytes[writable..];
        }
    }

    fn flush_block(&mut self) {
        let pool = unsafe { &mut *self.pool_ptr };
        let block_id = pool.disk_manager.allocate_anon_blocks(1);
        pool.disk_manager.write_page(block_id, &self.current_block);
        self.block_ids.push(block_id);
        self.current_block.fill(0);
        self.offset = 0;
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
    _marker: std::marker::PhantomData<&'a mut BufferPoolManager<R, W>>,
}

impl<'a, R: Read, W: Write> ScratchRunReader<'a, R, W> {
    fn new(pool_ptr: *mut BufferPoolManager<R, W>, block_ids: Vec<u64>, total_bytes: usize, block_size: usize) -> Self {
        Self { pool_ptr, block_ids, total_bytes, bytes_read: 0, block_idx: 0, current_block: vec![0; block_size], offset: 0, loaded: false, _marker: std::marker::PhantomData }
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
            let block_id = *self.block_ids.get(self.block_idx)?;
            let pool = unsafe { &mut *self.pool_ptr };
            pool.disk_manager.read_page(block_id, &mut self.current_block);
            self.block_idx += 1;
            self.offset = 0;
            self.loaded = true;
        }
        Some(())
    }
}

impl<'a, R: Read, W: Write> Drop for ScratchRunReader<'a, R, W> {
    fn drop(&mut self) {
        let pool = unsafe { &mut *self.pool_ptr };
        for &id in &self.block_ids {
            pool.disk_manager.free_anon_block(id);
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

// ==========================================
// SCAN OPERATOR
// ==========================================
pub struct ScanOperator<'a, R: Read, W: Write> {
    table_spec: &'a TableSpec,
    buffer_pool: &'a mut BufferPoolManager<R, W>,
    current_block_id: u64,
    end_block_id: u64,
    current_rows: std::vec::IntoIter<Row>,
}

impl<'a, R: Read, W: Write> ScanOperator<'a, R, W> {
    pub fn new(table_id: &str, table_spec: &'a TableSpec, buffer_pool: &'a mut BufferPoolManager<R, W>) -> Self {
        let start_block = buffer_pool.disk_manager.get_file_start_block(table_id);
        let num_blocks = buffer_pool.disk_manager.get_file_num_blocks(table_id);
        Self { table_spec, buffer_pool, current_block_id: start_block, end_block_id: start_block + num_blocks, current_rows: Vec::new().into_iter() }
    }
}

impl<'a, R: Read, W: Write> Operator for ScanOperator<'a, R, W> {
    fn next(&mut self) -> Option<Row> {
        loop {
            if let Some(row) = self.current_rows.next() { return Some(row); }
            self.current_rows = Vec::new().into_iter();

            if self.current_block_id < self.end_block_id {
                let frame_id = self.buffer_pool.fetch_page(self.current_block_id).unwrap();
                let data = self.buffer_pool.get_frame_data(frame_id);
                let rows = crate::data::deserialize_block(data, &self.table_spec.column_specs);
                self.current_rows = rows.into_iter();
                self.buffer_pool.unpin_page(self.current_block_id, false);
                self.current_block_id += 1;
            } else {
                return None;
            }
        }
    }
}

// ==========================================
// FILTER OPERATOR
// ==========================================
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
    let as_f64 = |val: &Value| -> Option<f64> {
        match val {
            Value::Int32(v) => Some(*v as f64), Value::Int64(v) => Some(*v as f64),
            Value::Float32(v) => Some(*v as f64), Value::Float64(v) => Some(*v),
            Value::String(_) => None,
        }
    };
    if let (Some(lhs_num), Some(rhs_num)) = (as_f64(lhs_val), as_f64(rhs_val)) {
        return match operator {
            ComparisionOperator::EQ => lhs_num == rhs_num, ComparisionOperator::NE => lhs_num != rhs_num,
            ComparisionOperator::GT => lhs_num > rhs_num, ComparisionOperator::LT => lhs_num < rhs_num,
            ComparisionOperator::GTE => lhs_num >= rhs_num, ComparisionOperator::LTE => lhs_num <= rhs_num,
        };
    }
    match operator {
        ComparisionOperator::EQ => lhs_val == rhs_val, ComparisionOperator::NE => lhs_val != rhs_val,
        ComparisionOperator::GT => lhs_val > rhs_val, ComparisionOperator::LT => lhs_val < rhs_val,
        ComparisionOperator::GTE => lhs_val >= rhs_val, ComparisionOperator::LTE => lhs_val <= rhs_val,
    }
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
            // High Performance String Comparison directly on references
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

// ==========================================
// PROJECT OPERATOR
// ==========================================
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

// ==========================================
// SORT OPERATOR
// ==========================================
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
        std::mem::size_of::<Row>() + (row.values.capacity() * std::mem::size_of::<Value>())
    }

    fn merge_fan_in(&self) -> usize {
        let per_run_bytes = self.scratch_block_size + 256;
        let budgeted_runs = self.memory_limit_bytes / per_run_bytes;
        budgeted_runs.max(2)
    }

    fn merge_runs_batch(&self, runs: Vec<ScratchRun>) -> ScratchRun {
        let mut readers = Vec::with_capacity(runs.len());
        let mut heap = BinaryHeap::new();
        for (run_idx, run) in runs.into_iter().enumerate() {
            let mut reader = ScratchRunReader::new(self.scratch_pool_ptr, run.block_ids, run.total_bytes, self.scratch_block_size);
            if let Some(row) = reader.read_row() { heap.push(HeapEntry::new(row, run_idx, &self.sort_indices)); }
            readers.push(reader);
        }
        let mut writer = ScratchRunWriter::new(self.scratch_pool_ptr, self.scratch_block_size);
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
                    let mut writer = ScratchRunWriter::new(self.scratch_pool_ptr, self.scratch_block_size);
                    for r in &current_run { writer.write_row(r); }
                    self.runs.push(writer.finish());

                    current_run = Vec::new(); 
                    current_memory = 0;
                }
            }

            if !current_run.is_empty() {
                current_run.sort_by(|a, b| HeapEntry::compare_rows(&self.sort_indices, a, b));
                if self.is_external {
                    let mut writer = ScratchRunWriter::new(self.scratch_pool_ptr, self.scratch_block_size);
                    for r in &current_run { writer.write_row(r); }
                    self.runs.push(writer.finish());
                } else {
                    self.in_memory_rows = current_run.into_iter();
                }
            }

            if self.is_external {
                self.collapse_runs();
                for run in self.runs.drain(..) {
                    let mut reader = ScratchRunReader::new(self.scratch_pool_ptr, run.block_ids, run.total_bytes, self.scratch_block_size);
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

// ==========================================
// CROSS OPERATOR
// ==========================================
pub struct CrossOperator<'a> {
    left_child: Option<Box<dyn Operator + 'a>>,
    right_child: Option<Box<dyn Operator + 'a>>,
    buffered_rows: Vec<Row>,
    current_stream_row: Option<Row>,
    buffered_index: usize,
    materialize_left: bool,
    initialized: bool,
}

impl<'a> CrossOperator<'a> {
    pub fn new(left_child: Box<dyn Operator + 'a>, right_child: Box<dyn Operator + 'a>, materialize_left: bool) -> Self {
        Self { left_child: Some(left_child), right_child: Some(right_child), buffered_rows: Vec::new(), current_stream_row: None, buffered_index: 0, materialize_left, initialized: false }
    }
}

impl<'a> Operator for CrossOperator<'a> {
    fn next(&mut self) -> Option<Row> {
        if !self.initialized {
            if self.materialize_left {
                if let Some(mut left) = self.left_child.take() {
                    while let Some(row) = left.next() { self.buffered_rows.push(row); }
                }
                let right = self.right_child.as_mut().unwrap();
                self.current_stream_row = right.next();
            } else {
                if let Some(mut right) = self.right_child.take() {
                    while let Some(row) = right.next() { self.buffered_rows.push(row); }
                }
                let left = self.left_child.as_mut().unwrap();
                self.current_stream_row = left.next();
            }
            self.initialized = true;
        }

        if self.buffered_rows.is_empty() { return None; }

        loop {
            if self.current_stream_row.is_none() { return None; }

            if self.buffered_index < self.buffered_rows.len() {
                let stream_row = self.current_stream_row.as_ref().unwrap();
                let buffered_row = &self.buffered_rows[self.buffered_index];
                self.buffered_index += 1;

                let (left_row, right_row) = if self.materialize_left {
                    (buffered_row, stream_row)
                } else {
                    (stream_row, buffered_row)
                };
                return Some(Row::combine(left_row, right_row));
            } else {
                self.buffered_index = 0;
                if self.materialize_left {
                    let right = self.right_child.as_mut().unwrap();
                    self.current_stream_row = right.next();
                } else {
                    let left = self.left_child.as_mut().unwrap();
                    self.current_stream_row = left.next();
                }
            }
        }
    }
}

// ==========================================
// HASH JOIN OPERATOR
// ==========================================
pub struct HashJoinOperator<'a> {
    left_child: Option<Box<dyn Operator + 'a>>,
    right_child: Option<Box<dyn Operator + 'a>>,
    left_col_idx: usize,
    right_col_idx: usize,
    build_on_left: bool,
    build_rows: Vec<Row>,
    hash_table: HashMap<Value, Vec<usize>>,
    current_probe_row: Option<Row>,
    current_match_key: Option<Value>,
    current_match_index: usize,
    initialized: bool,
}

impl<'a> HashJoinOperator<'a> {
    pub fn new(left_child: Box<dyn Operator + 'a>, right_child: Box<dyn Operator + 'a>, left_col_idx: usize, right_col_idx: usize, build_on_left: bool) -> Self {
        Self { left_child: Some(left_child), right_child: Some(right_child), left_col_idx, right_col_idx, build_on_left, build_rows: Vec::new(), hash_table: HashMap::new(), current_probe_row: None, current_match_key: None, current_match_index: 0, initialized: false }
    }
}

impl<'a> Operator for HashJoinOperator<'a> {
    fn next(&mut self) -> Option<Row> {
        if !self.initialized {
            let mut build_child = if self.build_on_left { self.left_child.take() } else { self.right_child.take() }.unwrap();
            let build_idx = if self.build_on_left { self.left_col_idx } else { self.right_col_idx };

            while let Some(row) = build_child.next() {
                let row_idx = self.build_rows.len();
                self.build_rows.push(row);
                let key = self.build_rows[row_idx].values[build_idx].clone();
                self.hash_table.entry(key).or_insert_with(Vec::new).push(row_idx);
            }
            self.initialized = true;
            
            let probe_child = if self.build_on_left { self.right_child.as_mut() } else { self.left_child.as_mut() }.unwrap();
            self.current_probe_row = probe_child.next();
        }

        loop {
            let probe_row = self.current_probe_row.as_ref()?;

            if let Some(key) = self.current_match_key.as_ref() {
                if let Some(build_rows) = self.hash_table.get(key) {
                    if self.current_match_index < build_rows.len() {
                        let b_row = &self.build_rows[build_rows[self.current_match_index]];
                        self.current_match_index += 1;

                        let (left_row, right_row) = if self.build_on_left {
                            (b_row, probe_row)
                        } else {
                            (probe_row, b_row)
                        };
                        return Some(Row::combine(left_row, right_row));
                    }
                }

                let probe_child = if self.build_on_left { self.right_child.as_mut() } else { self.left_child.as_mut() }.unwrap();
                self.current_probe_row = probe_child.next();
                self.current_match_key = None;
                self.current_match_index = 0;
                continue;
            }

            let probe_idx = if self.build_on_left { self.right_col_idx } else { self.left_col_idx };
            let key = probe_row.values[probe_idx].clone();
            let probe_child = if self.build_on_left { self.right_child.as_mut() } else { self.left_child.as_mut() }.unwrap();

            if self.hash_table.contains_key(&key) {
                self.current_match_key = Some(key);
                self.current_match_index = 0;
            } else {
                self.current_probe_row = probe_child.next();
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
    
    // Contiguous Memory Optimization 
    in_memory_hash_table: HashMap<Value, Vec<usize>>,
    in_memory_build_rows: Vec<Row>,

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
            in_memory_hash_table: HashMap::new(), in_memory_build_rows: Vec::new(),
            current_probe_reader: None, current_probe_row: None, current_match_key: None, 
            current_match_index: 0, state: JoinState::Partitioning 
        }
    }

    fn calculate_hash(val: &Value) -> usize {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;
        let mut s = DefaultHasher::new();
        val.hash(&mut s);
        s.finish() as usize
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
                        build_writers.push(ScratchRunWriter::new(self.scratch_pool_ptr, self.scratch_block_size));
                        probe_writers.push(ScratchRunWriter::new(self.scratch_pool_ptr, self.scratch_block_size));
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
                    if self.build_partitions.is_empty() {
                        self.state = JoinState::Done;
                        return None;
                    }

                    self.in_memory_hash_table.clear();
                    self.in_memory_build_rows.clear();
                    
                    let build_run = self.build_partitions.remove(0); 
                    let mut reader = ScratchRunReader::new(self.scratch_pool_ptr, build_run.block_ids, build_run.total_bytes, self.scratch_block_size);
                    
                    let build_idx = if self.build_on_left { self.left_col_idx } else { self.right_col_idx };
                    while let Some(row) = reader.read_row() {
                        let key = row.values[build_idx].clone();
                        let row_idx = self.in_memory_build_rows.len();
                        self.in_memory_build_rows.push(row);
                        self.in_memory_hash_table.entry(key).or_insert_with(Vec::new).push(row_idx);
                    }
                    
                    let probe_run = self.probe_partitions.remove(0);
                    self.current_probe_reader = Some(ScratchRunReader::new(self.scratch_pool_ptr, probe_run.block_ids, probe_run.total_bytes, self.scratch_block_size));
                    self.current_probe_row = self.current_probe_reader.as_mut().unwrap().read_row();
                    self.state = JoinState::Probing;
                }
                
                JoinState::Probing => {
                    if self.current_probe_row.is_none() {
                        self.state = JoinState::LoadingBuild;
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