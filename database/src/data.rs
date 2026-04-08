use std::fmt;
use std::hash::{Hash, Hasher};
use common::DataType;
use db_config::table::ColumnSpec;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Value {
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Value::Int32(v) => {
                1u8.hash(state);
                v.hash(state);
            }
            Value::Int64(v) => {
                2u8.hash(state);
                v.hash(state);
            }
            Value::Float32(v) => {
                3u8.hash(state);
                v.to_bits().hash(state);
            }
            Value::Float64(v) => {
                4u8.hash(state);
                v.to_bits().hash(state);
            }
            Value::String(v) => {
                5u8.hash(state);
                v.hash(state);
            }
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Float64(v) => write!(f, "{:?}", v), 
            Value::Float32(v) => write!(f, "{:?}", v),
            Value::Int32(v) => write!(f, "{}", v),
            Value::Int64(v) => write!(f, "{}", v),
            Value::String(v) => write!(f, "{}", v),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Row {
    pub values: Vec<Value>,
}

impl Row {
    pub fn to_output_string(&self) -> String {
        let mut s = String::new();
        for val in &self.values {
            s.push_str(&val.to_string());
            s.push_str("|");
        }
        s.push_str("\n");
        s
    }
}

pub fn deserialize_block(data: &[u8], columns: &[ColumnSpec]) -> Vec<Row> {
    let mut rows = Vec::new();
    if data.len() < 2 { return rows; }

    let map_capacity = data.len();
    let mut offset = 0;

    while offset < map_capacity {
    
        if offset + 8 <= map_capacity && data[offset..offset+8].iter().all(|&b| b == 0) {
            break;
        }

        let mut values = Vec::new();
        let mut row_valid = true;

        for col in columns {
            match col.data_type {
                DataType::Int64 => {
                    if offset + 8 > map_capacity { row_valid = false; break; }
                    let val = i64::from_le_bytes(data[offset..offset+8].try_into().unwrap());
                    values.push(Value::Int64(val));
                    offset += 8;
                }
                DataType::Float64 => {
                    if offset + 8 > map_capacity { row_valid = false; break; }
                    let val = f64::from_le_bytes(data[offset..offset+8].try_into().unwrap());
                    values.push(Value::Float64(val));
                    offset += 8;
                }
                DataType::Int32 => {
                    if offset + 4 > map_capacity { row_valid = false; break; }
                    let val = i32::from_le_bytes(data[offset..offset+4].try_into().unwrap());
                    values.push(Value::Int32(val));
                    offset += 4;
                }
                DataType::Float32 => {
                    if offset + 4 > map_capacity { row_valid = false; break; }
                    let val = f32::from_le_bytes(data[offset..offset+4].try_into().unwrap());
                    values.push(Value::Float32(val));
                    offset += 4;
                }
                DataType::String => {
                    let mut end = offset;
                    while end < map_capacity && data[end] != 0 {
                        end += 1;
                    }
                    if end >= map_capacity { row_valid = false; break; }
                    let s = String::from_utf8_lossy(&data[offset..end]).to_string();
                    values.push(Value::String(s));
                    offset = end + 1;
                }
            }
        }

        if row_valid && values.len() == columns.len() {
            rows.push(Row { values });
        } else {
            break; 
        }
    }
    
    rows
}
