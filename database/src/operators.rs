use std::io::{Read, Write};
use std::collections::HashMap;
use crate::data::{Row, Value};
use crate::buffer_pool::BufferPoolManager;
use common::query::{Predicate, ComparisionValue, ComparisionOperator};
use db_config::table::TableSpec;
use std::cmp::Ordering;

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
        Self {
            pool_ptr,
            block_size,
            current_block: vec![0; block_size],
            offset: 0,
            block_ids: Vec::new(),
            total_bytes: 0,
            _marker: std::marker::PhantomData,
        }
    }

    fn write_row(&mut self, row: &Row) {
        let encoded = row.encode();
        let row_len = u32::try_from(encoded.len()).expect("External Sort: Row too large to spill");
        self.write_bytes(&row_len.to_le_bytes());
        self.write_bytes(&encoded);
    }

    fn finish(mut self) -> ScratchRun {
        if self.offset > 0 {
            self.flush_block();
        }
        ScratchRun {
            block_ids: self.block_ids,
            total_bytes: self.total_bytes,
        }
    }

    fn write_bytes(&mut self, mut bytes: &[u8]) {
        self.total_bytes += bytes.len();
        while !bytes.is_empty() {
            if self.offset == self.block_size {
                self.flush_block();
            }

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
        Self {
            pool_ptr,
            block_ids,
            total_bytes,
            bytes_read: 0,
            block_idx: 0,
            current_block: vec![0; block_size],
            offset: 0,
            loaded: false,
            _marker: std::marker::PhantomData,
        }
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


pub trait Operator {
    fn next(&mut self) -> Option<Row>;
}

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
        }
    }
}

impl<'a, R: Read, W: Write> Operator for ScanOperator<'a, R, W> {
    fn next(&mut self) -> Option<Row> {
        loop {
            // Yield existing rows from the current block
            if let Some(row) = self.current_rows.next() {
                return Some(row);
            }

            self.current_rows = Vec::new().into_iter();

            if self.current_block_id < self.end_block_id {
                let frame_id = match self.buffer_pool.fetch_page(self.current_block_id) {
                    Ok(id) => id,
                    Err(e) => panic!("Buffer pool failed on block {}: {}", self.current_block_id, e),
                };
                
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
    pub fn new(child: Box<dyn Operator + 'a>, predicates: Vec<Predicate>, schema_map: HashMap<String, usize>) -> Self {
        let predicates = predicates
            .into_iter()
            .map(|pred| CompiledPredicate {
                lhs_idx: *schema_map
                    .get(&pred.column_name)
                    .expect("Column not found"),
                operator: pred.operator,
                rhs: match pred.value {
                    ComparisionValue::I32(v) => CompiledValue::I32(v),
                    ComparisionValue::I64(v) => CompiledValue::I64(v),
                    ComparisionValue::F32(v) => CompiledValue::F32(v),
                    ComparisionValue::F64(v) => CompiledValue::F64(v),
                    ComparisionValue::String(v) => CompiledValue::String(v),
                    ComparisionValue::Column(col_name) => CompiledValue::Column(
                        *schema_map.get(&col_name).expect("RHS Column not found in join")
                    ),
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
            let mut passes = true;
            for pred in &self.predicates {
                if !evaluate_predicate(&row, pred) {
                    passes = false;
                    break;
                }
            }
            if passes { return Some(row); }
        }
    }
}

fn compare_values(lhs_val: &Value, rhs_val: &Value, operator: &ComparisionOperator) -> bool {
    let as_f64 = |val: &Value| -> Option<f64> {
        match val {
            Value::Int32(v) => Some(*v as f64),
            Value::Int64(v) => Some(*v as f64),
            Value::Float32(v) => Some(*v as f64),
            Value::Float64(v) => Some(*v),
            Value::String(_) => None,
        }
    };

    if let (Some(lhs_num), Some(rhs_num)) = (as_f64(lhs_val), as_f64(rhs_val)) {
        return match operator {
            ComparisionOperator::EQ => lhs_num == rhs_num,
            ComparisionOperator::NE => lhs_num != rhs_num,
            ComparisionOperator::GT => lhs_num > rhs_num,
            ComparisionOperator::LT => lhs_num < rhs_num,
            ComparisionOperator::GTE => lhs_num >= rhs_num,
            ComparisionOperator::LTE => lhs_num <= rhs_num,
        };
    }

    match operator {
        ComparisionOperator::EQ => lhs_val == rhs_val,
        ComparisionOperator::NE => lhs_val != rhs_val,
        ComparisionOperator::GT => lhs_val > rhs_val,
        ComparisionOperator::LT => lhs_val < rhs_val,
        ComparisionOperator::GTE => lhs_val >= rhs_val,
        ComparisionOperator::LTE => lhs_val <= rhs_val,
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
        CompiledValue::String(v) => compare_values(lhs_val, &Value::String(v.clone()), &predicate.operator),
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
        for &idx in &self.column_indices {
            values.push(row.values[idx].clone());
        }
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
    
    // Fallback: If it fits in memory, we just use this
    in_memory_rows: std::vec::IntoIter<Row>,
    
    // External Sort State
    is_external: bool,
    scratch_pool_ptr: *mut BufferPoolManager<R, W>,
    scratch_block_size: usize,
    runs: Vec<ScratchRun>,
    run_readers: Vec<ScratchRunReader<'a, R, W>>,
    head_rows: Vec<Option<Row>>, // The current top row of each temp file
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
            head_rows: Vec::new(),
        }
    }

    //Compare two rows based on sort_indices
    fn compare_rows(&self, a: &Row, b: &Row) -> Ordering {
        for &(idx, ascending) in &self.sort_indices {
            let cmp = a.values[idx].partial_cmp(&b.values[idx]).unwrap_or(Ordering::Equal);
            if cmp != Ordering::Equal {
                return if ascending { cmp } else { cmp.reverse() };
            }
        }
        Ordering::Equal
    }

    //Estimate memory size of a row for spill decisions. This is a simple heuristic and can be improved.
    fn estimate_row_size(row: &Row) -> usize {
        let mut size = std::mem::size_of::<Row>() + (row.values.capacity() * std::mem::size_of::<Value>());
        for value in &row.values {
            if let Value::String(s) = value {
                size += s.capacity();
            }
        }
        size
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
                    current_run.sort_by(|a, b| self.compare_rows(a, b));
                    let mut writer = ScratchRunWriter::new(self.scratch_pool_ptr, self.scratch_block_size);
                    for r in &current_run {
                        writer.write_row(r);
                    }
                    self.runs.push(writer.finish());

                    current_run = Vec::new(); 
                    current_memory = 0;
                }
            }

            if !current_run.is_empty() {
                current_run.sort_by(|a, b| self.compare_rows(a, b));
                if self.is_external {
                    let mut writer = ScratchRunWriter::new(self.scratch_pool_ptr, self.scratch_block_size);
                    for r in &current_run {
                        writer.write_row(r);
                    }
                    self.runs.push(writer.finish());
                } else {
                    self.in_memory_rows = current_run.into_iter();
                }
            }

            if self.is_external {
                for run in self.runs.drain(..) {
                    let mut reader = ScratchRunReader::new(
                        self.scratch_pool_ptr,
                        run.block_ids,
                        run.total_bytes,
                        self.scratch_block_size,
                    );

                    if let Some(row) = reader.read_row() {
                        self.head_rows.push(Some(row));
                    } else {
                        self.head_rows.push(None);
                    }
                    self.run_readers.push(reader);
                }
            }
            self.initialized = true;
        }

        if !self.is_external {
            return self.in_memory_rows.next();
        }

        let mut min_idx = None;
        for i in 0..self.head_rows.len() {
            if let Some(ref current_head) = self.head_rows[i] {
                match min_idx {
                    None => min_idx = Some(i),
                    Some(idx) => {
                        let min_head = self.head_rows[idx].as_ref().unwrap();
                        if self.compare_rows(current_head, min_head) == Ordering::Less {
                            min_idx = Some(i);
                        }
                    }
                }
            }
        }

        if let Some(idx) = min_idx {
            let smallest_row = self.head_rows[idx].take(); 
            self.head_rows[idx] = self.run_readers[idx].read_row();
            return smallest_row;
        }

        None
    }
}


// ==========================================
// CROSS OPERATOR
// ==========================================
pub struct CrossOperator<'a> {
    left_child: Box<dyn Operator + 'a>,
    right_child: Option<Box<dyn Operator + 'a>>, 
    right_rows: Vec<Row>, 
    current_left_row: Option<Row>,
    right_index: usize,
    initialized: bool,
}

impl<'a> CrossOperator<'a> {
    pub fn new(left_child: Box<dyn Operator + 'a>, right_child: Box<dyn Operator + 'a>) -> Self {
        Self {
            left_child,
            right_child: Some(right_child),
            right_rows: Vec::new(),
            current_left_row: None,
            right_index: 0,
            initialized: false,
        }
    }
}

impl<'a> Operator for CrossOperator<'a> {
    fn next(&mut self) -> Option<Row> {
        if !self.initialized {
            // Materialize the right child into memory
            if let Some(mut right) = self.right_child.take() {
                while let Some(row) = right.next() {
                    self.right_rows.push(row);
                }
            }
            self.initialized = true;
            self.current_left_row = self.left_child.next();
        }

        if self.right_rows.is_empty() {
            return None;
        }

        loop {

            if self.current_left_row.is_none() {
                return None; 
            }

            if self.right_index < self.right_rows.len() {
                
                let left_row = self.current_left_row.as_ref().unwrap();
                let right_row = &self.right_rows[self.right_index];
                self.right_index += 1;

                let mut combined_values = Vec::with_capacity(left_row.values.len() + right_row.values.len());
                combined_values.extend(left_row.values.clone());
                combined_values.extend(right_row.values.clone());
                
                return Some(Row { values: combined_values });
            } else {
                self.current_left_row = self.left_child.next();
                self.right_index = 0;
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
    hash_table: HashMap<Value, Vec<Row>>, 
    current_probe_row: Option<Row>,
    current_match_key: Option<Value>,
    current_match_index: usize,
    initialized: bool,
}

impl<'a> HashJoinOperator<'a> {
    pub fn new(
        left_child: Box<dyn Operator + 'a>, 
        right_child: Box<dyn Operator + 'a>, 
        left_col_idx: usize, 
        right_col_idx: usize,
        build_on_left: bool, 
    ) -> Self {
        Self {
            left_child: Some(left_child),
            right_child: Some(right_child),
            left_col_idx,
            right_col_idx,
            build_on_left,
            hash_table: HashMap::new(),
            current_probe_row: None,
            current_match_key: None,
            current_match_index: 0,
            initialized: false,
        }
    }
}

impl<'a> Operator for HashJoinOperator<'a> {
    fn next(&mut self) -> Option<Row> {
        if !self.initialized {
            // BUILD PHASE
            let mut build_child = if self.build_on_left { self.left_child.take() } else { self.right_child.take() }.unwrap();
            let b_idx = if self.build_on_left { self.left_col_idx } else { self.right_col_idx };

            while let Some(row) = build_child.next() {
                let key = row.values[b_idx].clone();
                self.hash_table.entry(key).or_insert_with(Vec::new).push(row);
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
                        let b_row = &build_rows[self.current_match_index];
                        self.current_match_index += 1;

                        let mut vals = Vec::with_capacity(b_row.values.len() + probe_row.values.len());
                        if self.build_on_left {
                            vals.extend(b_row.values.clone());
                            vals.extend(probe_row.values.clone());
                        } else {
                            vals.extend(probe_row.values.clone());
                            vals.extend(b_row.values.clone());
                        }
                        return Some(Row { values: vals });
                    }
                }

                let probe_child = if self.build_on_left { self.right_child.as_mut() } else { self.left_child.as_mut() }.unwrap();
                self.current_probe_row = probe_child.next();
                self.current_match_key = None;
                self.current_match_index = 0;
                continue;
            }

            let p_idx = if self.build_on_left { self.right_col_idx } else { self.left_col_idx };
            let key = probe_row.values[p_idx].clone();
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
