use super::Operator;
use crate::buffer_pool::BufferPoolManager;
use crate::data::{Row, Value, global_dict};
use common::query::{ComparisionOperator, ComparisionValue, Predicate};
use db_config::table::TableSpec;
use std::collections::HashMap;
use std::io::{Read, Write};

pub struct BlockZoneMap {
    pub min_values: HashMap<usize, Value>,
    pub max_values: HashMap<usize, Value>,
}

pub enum CompiledValue {
    Column(usize),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    String { raw: String, dict_id: u32 },
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
    pushed_predicates: Vec<CompiledPredicate>,
    zone_maps: HashMap<u64, BlockZoneMap>,
    required_column_mask: Vec<bool>,
    prefetch_limit: usize,
}

impl<'a, R: Read, W: Write> ScanOperator<'a, R, W> {
    pub fn new(
        table_id: &str,
        table_spec: &'a TableSpec,
        buffer_pool: &'a mut BufferPoolManager<R, W>,
        pushed_predicates: Vec<CompiledPredicate>,
        zone_maps: HashMap<u64, BlockZoneMap>,
        required_column_mask: Vec<bool>,
        prefetch_limit: usize,
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
            pushed_predicates,
            zone_maps,
            required_column_mask,
            prefetch_limit,
        }
    }

    #[inline(always)]
    fn can_skip_block(&self, block_id: u64) -> bool {
        let zone_map = match self.zone_maps.get(&block_id) {
            Some(zm) => zm,
            None => return false,
        };

        for pred in &self.pushed_predicates {
            let (min_val, max_val) = match (
                zone_map.min_values.get(&pred.lhs_idx),
                zone_map.max_values.get(&pred.lhs_idx),
            ) {
                (Some(min), Some(max)) => (min, max),
                _ => continue,
            };

            match &pred.rhs {
                CompiledValue::I32(v) => {
                    if Self::skip_logic(&Value::Int32(*v), min_val, max_val, &pred.operator) {
                        return true;
                    }
                }
                CompiledValue::I64(v) => {
                    if Self::skip_logic(&Value::Int64(*v), min_val, max_val, &pred.operator) {
                        return true;
                    }
                }
                CompiledValue::F32(v) => {
                    if Self::skip_logic(
                        &Value::Float32(v.to_bits()),
                        min_val,
                        max_val,
                        &pred.operator,
                    ) {
                        return true;
                    }
                }
                CompiledValue::F64(v) => {
                    if Self::skip_logic(
                        &Value::Float64(v.to_bits()),
                        min_val,
                        max_val,
                        &pred.operator,
                    ) {
                        return true;
                    }
                }
                CompiledValue::String { dict_id, .. } => {
                    let dict_val = Value::DictString(*dict_id);
                    if Self::skip_logic(&dict_val, min_val, max_val, &pred.operator) {
                        return true;
                    }
                }
                _ => continue,
            }
        }
        false
    }

    #[inline(always)]
    fn skip_logic(target: &Value, min: &Value, max: &Value, op: &ComparisionOperator) -> bool {
        match op {
            ComparisionOperator::EQ => target < min || target > max,
            ComparisionOperator::GT | ComparisionOperator::GTE => max < target,
            ComparisionOperator::LT | ComparisionOperator::LTE => min > target,
            _ => false,
        }
    }
}

impl<'a, R: Read, W: Write> Operator for ScanOperator<'a, R, W> {
    fn next(&mut self) -> Option<Row> {
        loop {
            while let Some(row) = self.current_rows.next() {
                if self
                    .pushed_predicates
                    .iter()
                    .all(|pred| evaluate_predicate(&row, pred))
                {
                    return Some(row);
                }
            }
            if self.prefetch_offset < self.blocks_in_buffer {
                let block_size = self.buffer_pool.disk_manager.block_size;
                let data = &self.prefetch_buffer
                    [self.prefetch_offset * block_size..(self.prefetch_offset + 1) * block_size];
                let rows = crate::data::deserialize_block(
                    data,
                    &self.table_spec.column_specs,
                    &self.required_column_mask,
                );
                self.current_rows = rows.into_iter();
                self.prefetch_offset += 1;
                continue;
            }
            if self.current_block_id < self.end_block_id {
                while self.current_block_id < self.end_block_id
                    && self.can_skip_block(self.current_block_id)
                {
                    self.current_block_id += 1;
                }
                if self.current_block_id >= self.end_block_id {
                    return None;
                }
                let blocks_to_read = (self.end_block_id - self.current_block_id)
                    .min(self.prefetch_limit as u64) as usize;
                self.buffer_pool.read_pages_direct_into(
                    self.current_block_id,
                    blocks_to_read,
                    &mut self.prefetch_buffer,
                );
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

pub fn compile_predicates(
    predicates: Vec<Predicate>,
    schema_map: &HashMap<String, usize>,
) -> Vec<CompiledPredicate> {
    predicates
        .into_iter()
        .map(|pred| CompiledPredicate {
            lhs_idx: *schema_map.get(&pred.column_name).unwrap(),
            operator: pred.operator,
            rhs: match pred.value {
                ComparisionValue::I32(v) => CompiledValue::I32(v),
                ComparisionValue::I64(v) => CompiledValue::I64(v),
                ComparisionValue::F32(v) => CompiledValue::F32(v),
                ComparisionValue::F64(v) => CompiledValue::F64(v),
                ComparisionValue::String(v) => CompiledValue::String {
                    dict_id: global_dict().get_or_intern(&v),
                    raw: v,
                },
                ComparisionValue::Column(c) => CompiledValue::Column(*schema_map.get(&c).unwrap()),
            },
        })
        .collect()
}

impl<'a> FilterOperator<'a> {
    pub fn new(
        child: Box<dyn Operator + 'a>,
        predicates: Vec<Predicate>,
        schema_map: HashMap<String, usize>,
    ) -> Self {
        let predicates = compile_predicates(predicates, &schema_map);
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

#[inline(always)]
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
            Value::Float32(bits) => Some(f32::from_bits(*bits) as f64),
            Value::Float64(bits) => Some(f64::from_bits(*bits)),
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

#[inline(always)]
fn evaluate_predicate(row: &Row, predicate: &CompiledPredicate) -> bool {
    let lhs_val = &row.values[predicate.lhs_idx];
    match &predicate.rhs {
        CompiledValue::Column(r) => compare_values(lhs_val, &row.values[*r], &predicate.operator),
        CompiledValue::I32(v) => compare_values(lhs_val, &Value::Int32(*v), &predicate.operator),
        CompiledValue::I64(v) => compare_values(lhs_val, &Value::Int64(*v), &predicate.operator),
        CompiledValue::F32(v) => {
            compare_values(lhs_val, &Value::Float32(v.to_bits()), &predicate.operator)
        }
        CompiledValue::F64(v) => {
            compare_values(lhs_val, &Value::Float64(v.to_bits()), &predicate.operator)
        }
        CompiledValue::String { raw, dict_id } => match lhs_val {
            Value::DictString(lhs_id) => match predicate.operator {
                ComparisionOperator::EQ => *lhs_id == *dict_id,
                ComparisionOperator::NE => *lhs_id != *dict_id,
                ComparisionOperator::GT => global_dict().resolve(*lhs_id).as_ref() > raw.as_str(),
                ComparisionOperator::LT => global_dict().resolve(*lhs_id).as_ref() < raw.as_str(),
                ComparisionOperator::GTE => global_dict().resolve(*lhs_id).as_ref() >= raw.as_str(),
                ComparisionOperator::LTE => global_dict().resolve(*lhs_id).as_ref() <= raw.as_str(),
            },
            Value::String(s) => {
                let s_str: &str = s.as_ref();
                let v_str: &str = raw.as_str();
                match predicate.operator {
                    ComparisionOperator::EQ => s_str == v_str,
                    ComparisionOperator::NE => s_str != v_str,
                    ComparisionOperator::GT => s_str > v_str,
                    ComparisionOperator::LT => s_str < v_str,
                    ComparisionOperator::GTE => s_str >= v_str,
                    ComparisionOperator::LTE => s_str <= v_str,
                }
            }
            _ => false,
        },
    }
}

pub enum ProjectSource {
    Index(usize),
    Literal(u32),
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
                ProjectSource::Literal(dict_id) => Value::DictString(*dict_id),
            })
            .collect();
        Some(Row { values })
    }
}
