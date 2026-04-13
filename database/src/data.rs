use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use common::DataType;
use db_config::table::ColumnSpec;

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum Value {
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(Arc<str>),
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Value::Int32(v) => { 1u8.hash(state); v.hash(state); }
            Value::Int64(v) => { 2u8.hash(state); v.hash(state); }
            Value::Float32(v) => { 3u8.hash(state); v.to_bits().hash(state); }
            Value::Float64(v) => { 4u8.hash(state); v.to_bits().hash(state); }
            Value::String(v) => { 5u8.hash(state); v.hash(state); }
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

#[derive(Clone, Debug)]
pub struct Row {
    pub values: Vec<Value>,
}

impl Row {
    pub fn combine(left: &Row, right: &Row) -> Self {
        let mut values = Vec::with_capacity(left.values.len() + right.values.len());
        values.extend_from_slice(&left.values);
        values.extend_from_slice(&right.values);
        Self { values }
    }

    pub fn to_output_string(&self) -> String {
        let mut s = String::new();
        for val in &self.values {
            s.push_str(&val.to_string());
            s.push_str("|");
        }
        s.push_str("\n");
        s
    }

    pub fn encode(&self) -> Vec<u8> {
        let values_len = u32::try_from(self.values.len()).expect("Too many values in row");
        let capacity = 4 + self.values.iter().map(Value::encoded_len).sum::<usize>();
        let mut out = Vec::with_capacity(capacity);
        out.extend_from_slice(&values_len.to_le_bytes());
        for value in &self.values {
            value.encode_into(&mut out);
        }
        out
    }

    pub fn decode(input: &[u8]) -> Option<Self> {
        let count_bytes: [u8; 4] = input.get(..4)?.try_into().ok()?;
        let count = u32::from_le_bytes(count_bytes) as usize;
        let mut values = Vec::with_capacity(count);
        let mut offset = 4;

        for _ in 0..count {
            values.push(Value::decode_from(input, &mut offset)?);
        }

        if offset == input.len() {
            Some(Self { values })
        } else {
            None
        }
    }
}

impl Value {
    fn encoded_len(&self) -> usize {
        match self {
            Self::Int32(_) | Self::Float32(_) => 1 + 4,
            Self::Int64(_) | Self::Float64(_) => 1 + 8,
            Self::String(v) => 1 + 4 + v.len(),
        }
    }

    fn encode_into(&self, out: &mut Vec<u8>) {
        match self {
            Self::Int32(v) => { out.push(1); out.extend_from_slice(&v.to_le_bytes()); }
            Self::Int64(v) => { out.push(2); out.extend_from_slice(&v.to_le_bytes()); }
            Self::Float32(v) => { out.push(3); out.extend_from_slice(&v.to_le_bytes()); }
            Self::Float64(v) => { out.push(4); out.extend_from_slice(&v.to_le_bytes()); }
            Self::String(v) => {
                out.push(5);
                let len = u32::try_from(v.len()).expect("Row string too large to encode");
                out.extend_from_slice(&len.to_le_bytes());
                out.extend_from_slice(v.as_bytes());
            }
        }
    }

    fn decode_from(input: &[u8], offset: &mut usize) -> Option<Self> {
        let tag = *input.get(*offset)?;
        *offset += 1;

        match tag {
            1 => {
                let bytes: [u8; 4] = input.get(*offset..*offset + 4)?.try_into().ok()?;
                *offset += 4;
                Some(Self::Int32(i32::from_le_bytes(bytes)))
            }
            2 => {
                let bytes: [u8; 8] = input.get(*offset..*offset + 8)?.try_into().ok()?;
                *offset += 8;
                Some(Self::Int64(i64::from_le_bytes(bytes)))
            }
            3 => {
                let bytes: [u8; 4] = input.get(*offset..*offset + 4)?.try_into().ok()?;
                *offset += 4;
                Some(Self::Float32(f32::from_le_bytes(bytes)))
            }
            4 => {
                let bytes: [u8; 8] = input.get(*offset..*offset + 8)?.try_into().ok()?;
                *offset += 8;
                Some(Self::Float64(f64::from_le_bytes(bytes)))
            }
            5 => {
                let len_bytes: [u8; 4] = input.get(*offset..*offset + 4)?.try_into().ok()?;
                *offset += 4;
                let len = u32::from_le_bytes(len_bytes) as usize;
                let bytes = input.get(*offset..*offset + len)?;
                *offset += len;
                let value = String::from_utf8(bytes.to_vec()).ok()?;
                Some(Self::String(value.into())) 
            }
            _ => None,
        }
    }
}

pub fn deserialize_block(data: &[u8], columns: &[ColumnSpec]) -> Vec<Row> {
    if data.len() < 2 { return Vec::new(); }
    let map_capacity = data.len() - 2;
    let row_count = u16::from_le_bytes([data[map_capacity], data[map_capacity + 1]]) as usize;
    let mut rows = Vec::with_capacity(row_count);
    let mut offset = 0;

    for _ in 0..row_count {
        let mut values = Vec::with_capacity(columns.len());
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
                    while end < map_capacity && data[end] != 0 { end += 1; }
                    if end >= map_capacity { row_valid = false; break; }
                    let s = match std::str::from_utf8(&data[offset..end]) {
                        Ok(s) => s.to_owned(),
                        Err(_) => { row_valid = false; break; }
                    };
                    values.push(Value::String(s.into())); 
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