use tesseract_core::{ColumnData, Column};


/// DataFrame columns can come in many different types. This function converts
/// all data to a common type (String).
pub fn format_column_data(col: &Column) -> Vec<String> {
    // TODO: Fix rounding of numbers from xx.xx to xx

    return match &col.column_data {
        ColumnData::Int8(v) => {
            let mut t: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect();
            t.sort();
            t.iter().map(|&e| e.to_string()).collect()
        },
        ColumnData::Int16(v) => {
            let mut t: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect();
            t.sort();
            t.iter().map(|&e| e.to_string()).collect()
        },
        ColumnData::Int32(v) => {
            let mut t: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect();
            t.sort();
            t.iter().map(|&e| e.to_string()).collect()
        },
        ColumnData::Int64(v) => {
            let mut t: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect();
            t.sort();
            t.iter().map(|&e| e.to_string()).collect()
        },
        ColumnData::UInt8(v) => {
            let mut t: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect();
            t.sort();
            t.iter().map(|&e| e.to_string()).collect()
        },
        ColumnData::UInt16(v) => {
            let mut t: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect();
            t.sort();
            t.iter().map(|&e| e.to_string()).collect()
        },
        ColumnData::UInt32(v) => {
            let mut t: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect();
            t.sort();
            t.iter().map(|&e| e.to_string()).collect()
        },
        ColumnData::UInt64(v) => {
            let mut t: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect();
            t.sort();
            t.iter().map(|&e| e.to_string()).collect()
        },
        ColumnData::Float32(v) => {
            let mut t: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect();
            t.sort();
            t.iter().map(|&e| e.to_string()).collect()
        },
        ColumnData::Float64(v) => {
            let mut t: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect();
            t.sort();
            t.iter().map(|&e| e.to_string()).collect()
        },
        ColumnData::Text(v) => {
            let mut t = v.to_vec();
            t.sort();
            t
        },
        ColumnData::NullableInt8(v) => {
            let mut t: Vec<u32> = v.iter().filter_map(|&e| e.map(|e| e.clone() as u32)).collect();
            t.sort();
            t.iter().map(|&e| e.to_string()).collect()
        },
        ColumnData::NullableInt16(v) => {
            let mut t: Vec<u32> = v.iter().filter_map(|&e| e.map(|e| e.clone() as u32)).collect();
            t.sort();
            t.iter().map(|&e| e.to_string()).collect()
        },
        ColumnData::NullableInt32(v) => {
            let mut t: Vec<u32> = v.iter().filter_map(|&e| e.map(|e| e.clone() as u32)).collect();
            t.sort();
            t.iter().map(|&e| e.to_string()).collect()
        },
        ColumnData::NullableInt64(v) => {
            let mut t: Vec<u32> = v.iter().filter_map(|&e| e.map(|e| e.clone() as u32)).collect();
            t.sort();
            t.iter().map(|&e| e.to_string()).collect()
        },
        ColumnData::NullableUInt8(v) => {
            let mut t: Vec<u32> = v.iter().filter_map(|&e| e.map(|e| e.clone() as u32)).collect();
            t.sort();
            t.iter().map(|&e| e.to_string()).collect()
        },
        ColumnData::NullableUInt16(v) => {
            let mut t: Vec<u32> = v.iter().filter_map(|&e| e.map(|e| e.clone() as u32)).collect();
            t.sort();
            t.iter().map(|&e| e.to_string()).collect()
        },
        ColumnData::NullableUInt32(v) => {
            let mut t: Vec<u32> = v.iter().filter_map(|&e| e.map(|e| e.clone() as u32)).collect();
            t.sort();
            t.iter().map(|&e| e.to_string()).collect()
        },
        ColumnData::NullableUInt64(v) => {
            let mut t: Vec<u32> = v.iter().filter_map(|&e| e.map(|e| e.clone() as u32)).collect();
            t.sort();
            t.iter().map(|&e| e.to_string()).collect()
        },
        ColumnData::NullableFloat32(v) => {
            let mut t: Vec<u32> = v.iter().filter_map(|&e| e.map(|e| e.clone() as u32)).collect();
            t.sort();
            t.iter().map(|&e| e.to_string()).collect()
        },
        ColumnData::NullableFloat64(v) => {
            let mut t: Vec<u32> = v.iter().filter_map(|&e| e.map(|e| e.clone() as u32)).collect();
            t.sort();
            t.iter().map(|&e| e.to_string()).collect()
        },
        ColumnData::NullableText(v) => {
            let mut t: Vec<_> = v.into_iter().filter_map(|e| e.clone()).collect();
            t.sort();
            t
        },
    }
}
