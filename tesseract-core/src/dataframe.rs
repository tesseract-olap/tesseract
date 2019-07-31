#[derive(Debug)]
pub struct DataFrame {
    pub columns: Vec<Column>,
}

impl DataFrame {
    pub fn new() -> Self {
        DataFrame {
            columns: vec![],
        }
    }

    pub fn from_vec(columns: Vec<Column>) -> Self {
        DataFrame {
            columns
        }
    }

    pub fn len(&self) -> usize {
        if let Some(col) = self.columns.get(0) {
            match col.column_data {
                ColumnData::Int8(ref ns) => ns.len(),
                ColumnData::Int16(ref ns) => ns.len(),
                ColumnData::Int32(ref ns) => ns.len(),
                ColumnData::Int64(ref ns) => ns.len(),
                ColumnData::UInt8(ref ns) => ns.len(),
                ColumnData::UInt16(ref ns) => ns.len(),
                ColumnData::UInt32(ref ns) => ns.len(),
                ColumnData::UInt64(ref ns) => ns.len(),
                ColumnData::Float32(ref ns) => ns.len(),
                ColumnData::Float64(ref ns) => ns.len(),
                ColumnData::Text(ref ss) => ss.len(),
                ColumnData::NullableInt8(ref ns) => ns.len(),
                ColumnData::NullableInt16(ref ns) => ns.len(),
                ColumnData::NullableInt32(ref ns) => ns.len(),
                ColumnData::NullableInt64(ref ns) => ns.len(),
                ColumnData::NullableUInt8(ref ns) => ns.len(),
                ColumnData::NullableUInt16(ref ns) => ns.len(),
                ColumnData::NullableUInt32(ref ns) => ns.len(),
                ColumnData::NullableUInt64(ref ns) => ns.len(),
                ColumnData::NullableFloat32(ref ns) => ns.len(),
                ColumnData::NullableFloat64(ref ns) => ns.len(),
                ColumnData::NullableText(ref ss) => ss.len(),
            }
        } else {
            0
        }
    }
}

#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub column_data: ColumnData,
}

impl Column {
    pub fn new(name: String, column_data: ColumnData) -> Self {
        Column {
            name,
            column_data,
        }
    }

    pub fn column_data(&mut self) ->&mut ColumnData {
        &mut self.column_data
    }

    /// DataFrame columns can come in many different types. This function converts
    /// all data to a common type (String).
    pub fn stringify_column_data(&self) -> Vec<String> {
        return match &self.column_data {
            ColumnData::Int8(v) => {
                let mut t: Vec<i8> = v.iter().map(|&e| e.clone() as i8).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::Int16(v) => {
                let mut t: Vec<i16> = v.iter().map(|&e| e.clone() as i16).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::Int32(v) => {
                let mut t: Vec<i32> = v.iter().map(|&e| e.clone() as i32).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::Int64(v) => {
                let mut t: Vec<i64> = v.iter().map(|&e| e.clone() as i64).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::UInt8(v) => {
                let mut t: Vec<u8> = v.iter().map(|&e| e.clone() as u8).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::UInt16(v) => {
                let mut t: Vec<u16> = v.iter().map(|&e| e.clone() as u16).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::UInt32(v) => {
                let mut t: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::UInt64(v) => {
                let mut t: Vec<u64> = v.iter().map(|&e| e.clone() as u64).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::Float32(v) => {
                let t: Vec<f32> = v.iter().map(|&e| e.clone() as f32).collect();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::Float64(v) => {
                let t: Vec<f64> = v.iter().map(|&e| e.clone() as f64).collect();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::Text(v) => {
                let mut t = v.to_vec();
                t.sort();
                t
            },
            ColumnData::NullableInt8(v) => {
                let mut t: Vec<i8> = v.iter().filter_map(|&e| e.map(|e| e.clone() as i8)).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::NullableInt16(v) => {
                let mut t: Vec<i16> = v.iter().filter_map(|&e| e.map(|e| e.clone() as i16)).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::NullableInt32(v) => {
                let mut t: Vec<i32> = v.iter().filter_map(|&e| e.map(|e| e.clone() as i32)).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::NullableInt64(v) => {
                let mut t: Vec<i64> = v.iter().filter_map(|&e| e.map(|e| e.clone() as i64)).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::NullableUInt8(v) => {
                let mut t: Vec<u8> = v.iter().filter_map(|&e| e.map(|e| e.clone() as u8)).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::NullableUInt16(v) => {
                let mut t: Vec<u16> = v.iter().filter_map(|&e| e.map(|e| e.clone() as u16)).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::NullableUInt32(v) => {
                let mut t: Vec<u32> = v.iter().filter_map(|&e| e.map(|e| e.clone() as u32)).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::NullableUInt64(v) => {
                let mut t: Vec<u64> = v.iter().filter_map(|&e| e.map(|e| e.clone() as u64)).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::NullableFloat32(v) => {
                let t: Vec<f32> = v.iter().filter_map(|&e| e.map(|e| e.clone() as f32)).collect();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::NullableFloat64(v) => {
                let t: Vec<f64> = v.iter().filter_map(|&e| e.map(|e| e.clone() as f64)).collect();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::NullableText(v) => {
                let mut t: Vec<_> = v.into_iter().filter_map(|e| e.clone()).collect();
                t.sort();
                t
            },
        }
    }
}

#[derive(Debug)]
pub enum ColumnData {
    Int8(Vec<i8>),
    Int16(Vec<i16>),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    UInt8(Vec<u8>),
    UInt16(Vec<u16>),
    UInt32(Vec<u32>),
    UInt64(Vec<u64>),
    Float32(Vec<f32>),
    Float64(Vec<f64>),
    Text(Vec<String>),
    NullableInt8(Vec<Option<i8>>),
    NullableInt16(Vec<Option<i16>>),
    NullableInt32(Vec<Option<i32>>),
    NullableInt64(Vec<Option<i64>>),
    NullableUInt8(Vec<Option<u8>>),
    NullableUInt16(Vec<Option<u16>>),
    NullableUInt32(Vec<Option<u32>>),
    NullableUInt64(Vec<Option<u64>>),
    NullableFloat32(Vec<Option<f32>>),
    NullableFloat64(Vec<Option<f64>>),
    NullableText(Vec<Option<String>>),
}
