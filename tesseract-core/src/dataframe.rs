pub struct DataFrame {
    columns: Vec<Column>,
}

impl DataFrame {
    pub fn new() -> Self {
        DataFrame {
            columns: vec![],
        }
    }
}

pub struct Column {
    name: String,
    column_data: ColumnData,
}

impl Column {
    pub fn new(name: String, column_data: ColumnData) -> Self {
        Column {
            name,
            column_data,
        }
    }
}

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
}
