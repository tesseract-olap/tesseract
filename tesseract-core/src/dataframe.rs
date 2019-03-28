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
