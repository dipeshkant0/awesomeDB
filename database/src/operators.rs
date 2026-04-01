use std::io::{Read, Write};
use std::collections::HashMap;
use crate::data::{Row, Value};
use crate::buffer_pool::BufferPoolManager;
use common::query::{Predicate, ComparisionValue, ComparisionOperator};
use db_config::table::TableSpec;


pub trait Operator {
    fn next(&mut self) -> Option<Row>;
}


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
            if let Some(row) = self.current_rows.next() {
                return Some(row);
            }

            if self.current_block_id < self.end_block_id {
                
                if self.current_block_id % 500 == 0 {
                    eprintln!("Progress: Reading block {} of {}", self.current_block_id, self.end_block_id);
                }

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

pub struct FilterOperator<'a> {
    child: Box<dyn Operator + 'a>,
    predicates: Vec<Predicate>,
    schema_map: HashMap<String, usize>,
}

impl<'a> FilterOperator<'a> {
    pub fn new(child: Box<dyn Operator + 'a>, predicates: Vec<Predicate>, schema_map: HashMap<String, usize>) -> Self {
        Self { child, predicates, schema_map }
    }
}

impl<'a> Operator for FilterOperator<'a> {
    fn next(&mut self) -> Option<Row> {
        loop {
            let row = self.child.next()?;
            let mut passes = true;
            for pred in &self.predicates {
                if !evaluate_predicate(&row, pred, &self.schema_map) {
                    passes = false;
                    break;
                }
            }
            if passes { return Some(row); }
        }
    }
}

fn evaluate_predicate(row: &Row, predicate: &Predicate, schema_map: &HashMap<String, usize>) -> bool {
    let lhs_idx = schema_map.get(&predicate.column_name).expect("Column not found");
    let lhs_val = &row.values[*lhs_idx];

    let rhs_val = match &predicate.value {
        ComparisionValue::I32(v) => Value::Int32(*v),
        ComparisionValue::I64(v) => Value::Int64(*v),
        ComparisionValue::F32(v) => Value::Float32(*v),
        ComparisionValue::F64(v) => Value::Float64(*v),
        ComparisionValue::String(v) => Value::String(v.clone()),
        _ => panic!("Unsupported comparison value"),
    };

    match predicate.operator {
        ComparisionOperator::EQ => lhs_val == &rhs_val,
        ComparisionOperator::NE => lhs_val != &rhs_val,
        ComparisionOperator::GT => lhs_val > &rhs_val,
        ComparisionOperator::LT => lhs_val < &rhs_val,
        ComparisionOperator::GTE => lhs_val >= &rhs_val,
        ComparisionOperator::LTE => lhs_val <= &rhs_val,
    }
}

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
        let mut values = Vec::new();
        for &idx in &self.column_indices {
            values.push(row.values[idx].clone());
        }
        Some(Row { values })
    }
}