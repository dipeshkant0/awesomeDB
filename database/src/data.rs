use common::DataType;
use db_config::table::ColumnSpec;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, OnceLock, RwLock};

pub struct StringDictionary {
    map: RwLock<HashMap<Arc<str>, u32>>,
    list: RwLock<Vec<Arc<str>>>,
}

impl StringDictionary {
    pub fn new() -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
            list: RwLock::new(Vec::new()),
        }
    }

    pub fn get_or_intern(&self, s: &str) -> u32 {
        {
            let read = self.map.read().unwrap();
            if let Some(&id) = read.get(s) {
                return id;
            }
        }
        let mut write_map = self.map.write().unwrap();
        let mut write_list = self.list.write().unwrap();
        if let Some(&id) = write_map.get(s) {
            return id;
        }

        let id = write_list.len() as u32;
        let arc_s: Arc<str> = Arc::from(s);
        write_map.insert(arc_s.clone(), id);
        write_list.push(arc_s);
        id
    }

    pub fn resolve(&self, id: u32) -> Arc<str> {
        let read = self.list.read().unwrap();
        read[id as usize].clone()
    }
}

static GLOBAL_DICT_CELL: OnceLock<StringDictionary> = OnceLock::new();
pub fn global_dict() -> &'static StringDictionary {
    GLOBAL_DICT_CELL.get_or_init(StringDictionary::new)
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Value {
    Int32(i32),
    Int64(i64),
    Float32(u32),
    Float64(u64),
    String(Arc<str>),
    DictString(u32),
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::DictString(id1), Value::DictString(id2)) => {
                if id1 == id2 {
                    return Some(Ordering::Equal);
                }
                global_dict()
                    .resolve(*id1)
                    .partial_cmp(&global_dict().resolve(*id2))
            }
            (Value::DictString(id), Value::String(s)) => global_dict().resolve(*id).partial_cmp(s),
            (Value::String(s), Value::DictString(id)) => s.partial_cmp(&global_dict().resolve(*id)),
            (Value::Int32(a), Value::Int32(b)) => a.partial_cmp(b),
            (Value::Int64(a), Value::Int64(b)) => a.partial_cmp(b),
            (Value::Float32(a), Value::Float32(b)) => {
                f32::from_bits(*a).partial_cmp(&f32::from_bits(*b))
            }
            (Value::Float64(a), Value::Float64(b)) => {
                f64::from_bits(*a).partial_cmp(&f64::from_bits(*b))
            }
            (Value::String(a), Value::String(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Float64(v) => write!(f, "{:?}", f64::from_bits(*v)),
            Value::Float32(v) => write!(f, "{:?}", f32::from_bits(*v)),
            Value::Int32(v) => write!(f, "{}", v),
            Value::Int64(v) => write!(f, "{}", v),
            Value::String(v) => write!(f, "{}", v),
            Value::DictString(id) => write!(f, "{}", global_dict().resolve(*id)),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Row {
    pub values: Vec<Value>,
}

impl Row {
    pub fn combine(left: &Row, right: &Row) -> Self {
        let total = left.values.len() + right.values.len();
        let mut values = Vec::with_capacity(total);
        // Clone values; for Copy-like variants (Int32, Int64, Float32, Float64, DictString)
        // this is essentially a memcpy. Only String(Arc<str>) has ref-count overhead.
        values.extend_from_slice(&left.values);
        values.extend_from_slice(&right.values);
        Self { values }
    }

    pub fn to_output_string(&self) -> String {
        let mut s = String::with_capacity(self.values.len() * 16);
        for val in &self.values {
            use std::fmt::Write as _;
            let _ = write!(&mut s, "{}", val);
            s.push_str("|");
        }
        s.push_str("\n");
        s
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.encoded_len());
        self.encode_into(&mut out);
        out
    }

    pub fn encoded_len(&self) -> usize {
        4 + self.values.iter().map(Value::encoded_len).sum::<usize>()
    }

    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&(self.values.len() as u32).to_le_bytes());
        for value in &self.values {
            value.encode_into(out);
        }
    }

    pub fn write_encoded_chunks<F>(&self, mut sink: F)
    where
        F: FnMut(&[u8]),
    {
        let count = (self.values.len() as u32).to_le_bytes();
        sink(&count);
        for value in &self.values {
            value.write_encoded_chunks(&mut sink);
        }
    }

    pub fn decode(input: &[u8]) -> Option<Self> {
        let count = u32::from_le_bytes(input.get(..4)?.try_into().ok()?) as usize;
        let mut values = Vec::with_capacity(count);
        let mut offset = 4;
        for _ in 0..count {
            values.push(Value::decode_from(input, &mut offset)?);
        }
        Some(Self { values })
    }
}

impl Value {
    fn encoded_len(&self) -> usize {
        match self {
            Self::Int32(_) => 1 + 4,
            Self::Int64(_) => 1 + 8,
            Self::Float32(_) => 1 + 4,
            Self::Float64(_) => 1 + 8,
            Self::String(v) => 1 + 4 + v.len(),
            Self::DictString(_) => 1 + 4,
        }
    }

    fn encode_into(&self, out: &mut Vec<u8>) {
        match self {
            Self::Int32(v) => { out.push(1); out.extend_from_slice(&v.to_le_bytes()); }
            Self::Int64(v) => { out.push(2); out.extend_from_slice(&v.to_le_bytes()); }
            Self::Float32(v) => { out.push(3); out.extend_from_slice(&v.to_le_bytes()); }
            Self::Float64(v) => { out.push(4); out.extend_from_slice(&v.to_le_bytes()); }
            Self::String(v) => { out.push(5); out.extend_from_slice(&(v.len() as u32).to_le_bytes()); out.extend_from_slice(v.as_bytes()); }
            Self::DictString(id) => { out.push(6); out.extend_from_slice(&id.to_le_bytes()); }
        }
    }

    fn write_encoded_chunks<F>(&self, sink: &mut F)
    where
        F: FnMut(&[u8]),
    {
        match self {
            Self::Int32(v) => { sink(&[1]); sink(&v.to_le_bytes()); }
            Self::Int64(v) => { sink(&[2]); sink(&v.to_le_bytes()); }
            Self::Float32(v) => { sink(&[3]); sink(&v.to_le_bytes()); }
            Self::Float64(v) => { sink(&[4]); sink(&v.to_le_bytes()); }
            Self::String(v) => { sink(&[5]); sink(&(v.len() as u32).to_le_bytes()); sink(v.as_bytes()); }
            Self::DictString(id) => { sink(&[6]); sink(&id.to_le_bytes()); }
        }
    }

    fn decode_from(input: &[u8], offset: &mut usize) -> Option<Self> {
        let tag = *input.get(*offset)?;
        *offset += 1;
        match tag {
            1 => { let v = i32::from_le_bytes(input.get(*offset..*offset + 4)?.try_into().ok()?); *offset += 4; Some(Self::Int32(v)) }
            2 => { let v = i64::from_le_bytes(input.get(*offset..*offset + 8)?.try_into().ok()?); *offset += 8; Some(Self::Int64(v)) }
            3 => { let v = u32::from_le_bytes(input.get(*offset..*offset + 4)?.try_into().ok()?); *offset += 4; Some(Self::Float32(v)) }
            4 => { let v = u64::from_le_bytes(input.get(*offset..*offset + 8)?.try_into().ok()?); *offset += 8; Some(Self::Float64(v)) }
            5 => {
                let len = u32::from_le_bytes(input.get(*offset..*offset + 4)?.try_into().ok()?) as usize;
                *offset += 4;
                let s = std::str::from_utf8(input.get(*offset..*offset + len)?).ok()?;
                *offset += len;
                Some(Self::String(Arc::from(s)))
            }
            6 => { let id = u32::from_le_bytes(input.get(*offset..*offset + 4)?.try_into().ok()?); *offset += 4; Some(Self::DictString(id)) }
            _ => None,
        }
    }
}

pub fn deserialize_block(
    data: &[u8],
    columns: &[ColumnSpec],
    required_column_mask: &[bool],
) -> Vec<Row> {
    if data.len() < 2 {
        return Vec::new();
    }
    let map_capacity = data.len() - 2;
    let row_count = u16::from_le_bytes([data[map_capacity], data[map_capacity + 1]]) as usize;
    let mut rows = Vec::with_capacity(row_count);
    let mut offset = 0;

    for _ in 0..row_count {
        let mut values = Vec::with_capacity(columns.len());
        let mut row_valid = true;

        for (col_idx, col) in columns.iter().enumerate() {
            let is_required = required_column_mask.get(col_idx).copied().unwrap_or(false);
            match col.data_type {
                common::DataType::Int32 => {
                    // 🚀 RESTORED CRITICAL BOUNDS CHECK
                    if offset + 4 > map_capacity { row_valid = false; break; }
                    if is_required {
                        let bytes: [u8; 4] = data[offset..offset + 4].try_into().unwrap();
                        values.push(Value::Int32(i32::from_le_bytes(bytes)));
                    } else {
                        values.push(Value::Int32(0));
                    }
                    offset += 4;
                }
                common::DataType::Int64 => {
                    if offset + 8 > map_capacity { row_valid = false; break; }
                    if is_required {
                        let bytes: [u8; 8] = data[offset..offset + 8].try_into().unwrap();
                        values.push(Value::Int64(i64::from_le_bytes(bytes)));
                    } else {
                        values.push(Value::Int64(0));
                    }
                    offset += 8;
                }
                common::DataType::Float32 => {
                    if offset + 4 > map_capacity { row_valid = false; break; }
                    if is_required {
                        let bytes: [u8; 4] = data[offset..offset + 4].try_into().unwrap();
                        values.push(Value::Float32(u32::from_le_bytes(bytes)));
                    } else {
                        values.push(Value::Float32(0));
                    }
                    offset += 4;
                }
                common::DataType::Float64 => {
                    if offset + 8 > map_capacity { row_valid = false; break; }
                    if is_required {
                        let bytes: [u8; 8] = data[offset..offset + 8].try_into().unwrap();
                        values.push(Value::Float64(u64::from_le_bytes(bytes)));
                    } else {
                        values.push(Value::Float64(0));
                    }
                    offset += 8;
                }
                common::DataType::String => {
                    let mut end = offset;
                    while end < map_capacity && data[end] != 0 {
                        end += 1;
                    }
                    if end > map_capacity { row_valid = false; break; }
                    if is_required {
                        let s = std::str::from_utf8(&data[offset..end]).unwrap_or("");
                        // Intern into global dictionary — each unique string stored once,
                        // reducing memory from O(rows) to O(unique_strings).
                        let dict_id = global_dict().get_or_intern(s);
                        values.push(Value::DictString(dict_id));
                    } else {
                        values.push(Value::Int32(0));
                    }
                    offset = end + 1; // skip null terminator
                }
            }
        }
        
        // 🚀 ONLY EMIT PERFECTLY PACKED ROWS
        if row_valid && values.len() == columns.len() {
            rows.push(Row { values });
        } else {
            break;
        }
    }
    rows
}