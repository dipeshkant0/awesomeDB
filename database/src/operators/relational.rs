use super::Operator;
use crate::buffer_pool::BufferPoolManager;
use crate::data::{Row, Value};
use common::query::{ComparisionOperator, ComparisionValue, Predicate};
use db_config::table::TableSpec;
use std::collections::HashMap;
use std::io::{Read, Write};

pub enum CompiledValue {
    Column(usize),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    String(String),
}

pub struct CompiledPredicate {
    pub lhs_idx: usize,
    pub operator: ComparisionOperator,
    pub rhs: CompiledValue,
}

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
                let blocks_to_read = (self.end_block_id - self.current_block_id).min(1024) as usize;
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

pub struct FilterOperator<'a> {
    child: Box<dyn Operator + 'a>,
    predicates: Vec<CompiledPredicate>,
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

pub fn compare_values(lhs: &Value, rhs: &Value, op: &ComparisionOperator) -> bool {
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
