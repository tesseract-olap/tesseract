use csv;
use failure::{Error, format_err};
use indexmap::IndexMap;
use serde_json::{json, Value};

use crate::dataframe::{DataFrame, ColumnData};

#[derive(Debug, Clone)]
pub enum FormatType{
    Csv,
    JsonRecords,
    JsonArrays,
}

impl std::str::FromStr for FormatType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "csv" => Ok(FormatType::Csv),
            "jsonrecords" => Ok(FormatType::JsonRecords),
            "jsonarrays" => Ok(FormatType::JsonArrays),
            _ => Err(format_err!("{} is not a supported format", s)),
        }
    }
}

/// Wrapper to format `DataFrame` to the desired output format.
pub fn format_records(headers: &[String], df: DataFrame, format_type: FormatType) -> Result<String, Error> {
    match format_type {
        FormatType::Csv => Ok(format_csv(headers, df)?),
        FormatType::JsonRecords => Ok(format_jsonrecords(headers, df)?),
        FormatType::JsonArrays => Ok(format_jsonarrays(headers, df)?),
    }
}

/// Formats response `DataFrame` to CSV.
fn format_csv(headers: &[String], df: DataFrame) -> Result<String, Error> {
    let mut wtr = csv::WriterBuilder::new()
        .from_writer(vec![]);

    // write header
    wtr.write_record(headers)?;

    let mut row_buf = vec![];

    // write data
    for row_idx in 0..df.len() {
        for col_idx in 0..df.columns.len() {
            let val = match df.columns[col_idx].column_data {
                ColumnData::Int8(ref ns) =>    ns[row_idx].to_string(),
                ColumnData::Int16(ref ns) =>   ns[row_idx].to_string(),
                ColumnData::Int32(ref ns) =>   ns[row_idx].to_string(),
                ColumnData::Int64(ref ns) =>   ns[row_idx].to_string(),
                ColumnData::UInt8(ref ns) =>   ns[row_idx].to_string(),
                ColumnData::UInt16(ref ns) =>  ns[row_idx].to_string(),
                ColumnData::UInt32(ref ns) =>  ns[row_idx].to_string(),
                ColumnData::UInt64(ref ns) =>  ns[row_idx].to_string(),
                ColumnData::Float32(ref ns) => ns[row_idx].to_string(),
                ColumnData::Float64(ref ns) => ns[row_idx].to_string(),
                ColumnData::Text(ref ss) =>    ss[row_idx].to_string(),
                ColumnData::NullableInt8(ref ns) =>    ns[row_idx].map(|n| n.to_string()).unwrap_or("".into()),
                ColumnData::NullableInt16(ref ns) =>   ns[row_idx].map(|n| n.to_string()).unwrap_or("".into()),
                ColumnData::NullableInt32(ref ns) =>   ns[row_idx].map(|n| n.to_string()).unwrap_or("".into()),
                ColumnData::NullableInt64(ref ns) =>   ns[row_idx].map(|n| n.to_string()).unwrap_or("".into()),
                ColumnData::NullableUInt8(ref ns) =>   ns[row_idx].map(|n| n.to_string()).unwrap_or("".into()),
                ColumnData::NullableUInt16(ref ns) =>  ns[row_idx].map(|n| n.to_string()).unwrap_or("".into()),
                ColumnData::NullableUInt32(ref ns) =>  ns[row_idx].map(|n| n.to_string()).unwrap_or("".into()),
                ColumnData::NullableUInt64(ref ns) =>  ns[row_idx].map(|n| n.to_string()).unwrap_or("".into()),
                ColumnData::NullableFloat32(ref ns) => ns[row_idx].map(|n| n.to_string()).unwrap_or("".into()),
                ColumnData::NullableFloat64(ref ns) => ns[row_idx].map(|n| n.to_string()).unwrap_or("".into()),
                ColumnData::NullableText(ref ss) =>    ss[row_idx].clone().unwrap_or("".into()),
            };

            row_buf.push(val);
        }
        wtr.write_record(&row_buf)?;

        row_buf.clear();
    }

    let res = String::from_utf8(wtr.into_inner()?)?;

    Ok(res)
}

/// Formats response `DataFrame` to JSON records.
fn format_jsonrecords(headers: &[String], df: DataFrame) -> Result<String, Error> {
    // each HashMap is a row
    let mut rows = vec![];


    // write data
    for row_idx in 0..df.len() {
        let mut row: IndexMap<&str, serde_json::Value> = IndexMap::new();
        for col_idx in 0..df.columns.len() {
            let val = match df.columns[col_idx].column_data {
                ColumnData::Int8(ref ns) =>    ns[row_idx].clone().into(),
                ColumnData::Int16(ref ns) =>   ns[row_idx].clone().into(),
                ColumnData::Int32(ref ns) =>   ns[row_idx].clone().into(),
                ColumnData::Int64(ref ns) =>   ns[row_idx].clone().into(),
                ColumnData::UInt8(ref ns) =>   ns[row_idx].clone().into(),
                ColumnData::UInt16(ref ns) =>  ns[row_idx].clone().into(),
                ColumnData::UInt32(ref ns) =>  ns[row_idx].clone().into(),
                ColumnData::UInt64(ref ns) =>  ns[row_idx].clone().into(),
                ColumnData::Float32(ref ns) => ns[row_idx].clone().into(),
                ColumnData::Float64(ref ns) => ns[row_idx].clone().into(),
                ColumnData::Text(ref ss) =>    ss[row_idx].clone().into(),
                ColumnData::NullableInt8(ref ns) =>    ns[row_idx].map(|n| n.clone().into()).unwrap_or(Value::Null),
                ColumnData::NullableInt16(ref ns) =>   ns[row_idx].map(|n| n.clone().into()).unwrap_or(Value::Null),
                ColumnData::NullableInt32(ref ns) =>   ns[row_idx].map(|n| n.clone().into()).unwrap_or(Value::Null),
                ColumnData::NullableInt64(ref ns) =>   ns[row_idx].map(|n| n.clone().into()).unwrap_or(Value::Null),
                ColumnData::NullableUInt8(ref ns) =>   ns[row_idx].map(|n| n.clone().into()).unwrap_or(Value::Null),
                ColumnData::NullableUInt16(ref ns) =>  ns[row_idx].map(|n| n.clone().into()).unwrap_or(Value::Null),
                ColumnData::NullableUInt32(ref ns) =>  ns[row_idx].map(|n| n.clone().into()).unwrap_or(Value::Null),
                ColumnData::NullableUInt64(ref ns) =>  ns[row_idx].map(|n| n.clone().into()).unwrap_or(Value::Null),
                ColumnData::NullableFloat32(ref ns) => ns[row_idx].map(|n| n.clone().into()).unwrap_or(Value::Null),
                ColumnData::NullableFloat64(ref ns) => ns[row_idx].map(|n| n.clone().into()).unwrap_or(Value::Null),
                ColumnData::NullableText(ref ss) =>    ss[row_idx].clone().map(|n| n.into()).unwrap_or(Value::Null),
            };

            row.insert(&headers[col_idx], val);
        }

        rows.push(row);
    }

    let res = json!({
        "data": rows,
    });

    Ok(res.to_string())
}

/// Formats response `DataFrame` to JSON arrays.
fn format_jsonarrays(headers: &[String], df: DataFrame) -> Result<String, Error> {
    let mut rows = vec![];

    // then write data
    for row_idx in 0..df.len() {
        let mut row: Vec<serde_json::Value> = vec![];
        for col_idx in 0..df.columns.len() {
            let val = match df.columns[col_idx].column_data {
                ColumnData::Int8(ref ns) =>    ns[row_idx].clone().into(),
                ColumnData::Int16(ref ns) =>   ns[row_idx].clone().into(),
                ColumnData::Int32(ref ns) =>   ns[row_idx].clone().into(),
                ColumnData::Int64(ref ns) =>   ns[row_idx].clone().into(),
                ColumnData::UInt8(ref ns) =>   ns[row_idx].clone().into(),
                ColumnData::UInt16(ref ns) =>  ns[row_idx].clone().into(),
                ColumnData::UInt32(ref ns) =>  ns[row_idx].clone().into(),
                ColumnData::UInt64(ref ns) =>  ns[row_idx].clone().into(),
                ColumnData::Float32(ref ns) => ns[row_idx].clone().into(),
                ColumnData::Float64(ref ns) => ns[row_idx].clone().into(),
                ColumnData::Text(ref ss) =>    ss[row_idx].clone().into(),
                ColumnData::NullableInt8(ref ns) =>    ns[row_idx].map(|n| n.clone().into()).unwrap_or(Value::Null),
                ColumnData::NullableInt16(ref ns) =>   ns[row_idx].map(|n| n.clone().into()).unwrap_or(Value::Null),
                ColumnData::NullableInt32(ref ns) =>   ns[row_idx].map(|n| n.clone().into()).unwrap_or(Value::Null),
                ColumnData::NullableInt64(ref ns) =>   ns[row_idx].map(|n| n.clone().into()).unwrap_or(Value::Null),
                ColumnData::NullableUInt8(ref ns) =>   ns[row_idx].map(|n| n.clone().into()).unwrap_or(Value::Null),
                ColumnData::NullableUInt16(ref ns) =>  ns[row_idx].map(|n| n.clone().into()).unwrap_or(Value::Null),
                ColumnData::NullableUInt32(ref ns) =>  ns[row_idx].map(|n| n.clone().into()).unwrap_or(Value::Null),
                ColumnData::NullableUInt64(ref ns) =>  ns[row_idx].map(|n| n.clone().into()).unwrap_or(Value::Null),
                ColumnData::NullableFloat32(ref ns) => ns[row_idx].map(|n| n.clone().into()).unwrap_or(Value::Null),
                ColumnData::NullableFloat64(ref ns) => ns[row_idx].map(|n| n.clone().into()).unwrap_or(Value::Null),
                ColumnData::NullableText(ref ss) =>    ss[row_idx].clone().map(|n| n.into()).unwrap_or(Value::Null),
            };

            row.push(val);
        }

        rows.push(row);
    }

    let res = json!({
        "headers": headers,
        "data": rows,
    });

    Ok(res.to_string())
}
