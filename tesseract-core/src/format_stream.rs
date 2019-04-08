use bytes::Bytes;
use csv;
use failure::{Error, format_err};
use futures::{Stream, Async, Poll};
use indexmap::IndexMap;
use serde::Serializer;
use serde::ser::{SerializeSeq};
use serde_json::{Value};

use crate::dataframe::{DataFrame, ColumnData};
use super::format::FormatType;

/// Wrapper to format `DataFrame` to the desired output format.
pub fn format_records_stream<S>(headers: Vec<String>, df_stream: S, format_type: FormatType) -> RecordBlockStream<S>
    where
    S: Stream<Item=Result<DataFrame, Error>, Error=Error> + 'static
{
    RecordBlockStream::new(df_stream, headers, format_type)
}

pub struct RecordBlockStream<S>
    where S: Stream<Item=Result<DataFrame, Error>, Error=Error> + 'static
{
    inner: S,
    sent_header: bool,
    sent_footer: bool,
    format_type: FormatType,
    headers: Vec<String>,
}

impl<S> RecordBlockStream<S>
    where S: Stream<Item=Result<DataFrame, Error>, Error=Error> + 'static
{
    pub fn new(stream: S, headers: Vec<String>, format_type: FormatType) -> Self {
        RecordBlockStream {
            inner: stream,
            sent_header: false,
            sent_footer: false,
            format_type,
            headers,
        }
    }
}

impl<S> Stream for RecordBlockStream<S>
    where S: Stream<Item=Result<DataFrame, Error>, Error=Error> + 'static
{
    type Item = Bytes;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // first look at header
        if !self.sent_header {
            match self.format_type {
                FormatType::Csv => {
                    let mut wtr = csv::WriterBuilder::new()
                        .from_writer(vec![]);

                    wtr.write_record(&self.headers)?;

                    let buf = wtr.into_inner()?;
                    let bytes: Bytes = buf.into();

                    self.sent_header = true;

                    return Ok(Async::Ready(Some(bytes)));
                },
                _ => return Err(format_err!("just csv first")),
                //FormatType::JsonRecords => format_csv(headers, df_stream),
                //FormatType::JsonArrays => format_csv(headers, df_stream),
            }
        }

        loop {
            let df_res = match self.inner.poll() {
                Err(err) => return Err(err),
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Ok(Async::Ready(None)) => return Ok(Async::Ready(None)),
                Ok(Async::Ready(Some(df_res))) => df_res,
            };

            match df_res {
                Ok(df) => {
                    let formatted = match self.format_type {
                        FormatType::Csv => {
                            format_csv_body(df)?
                        },
                        //FormatType::JsonRecords => format_csv(headers, df_stream),
                        //FormatType::JsonArrays => format_csv(headers, df_stream),
                        _ => return Err(format_err!("just csv first")),
                    };

                    return Ok(Async::Ready(Some(formatted)));
                },
                Err(err) => return Err(err),

            }
        }
    }
}


/// Formats response `DataFrame` to CSV.
fn format_csv_body(df: DataFrame) -> Result<Bytes, Error>
{
    let mut wtr = csv::WriterBuilder::new()
        .from_writer(vec![]);
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
        wtr.write_record(&row_buf);

        row_buf.clear();
    }

    let buf = wtr.into_inner()?;
    let bytes = buf.into();

    Ok(bytes)
}

/// Formats response `DataFrame` to JSON records.
fn format_jsonrecords(headers: &[String], df: DataFrame) -> Result<String, Error> {
    // use streaming serializer
    // Necessary because this way we don't create a huge vec of rows containing Value
    // (very expensive)

    // I've ended up doing this a bit more manually than I want; but I don't know how
    // to easily move between manually calling serializing methods, and having some
    // done more automatically. For example, the rows have to be serialized using
    // seq, but I can't easily call serialize_data including those rows, since I'm using
    // a manual method different from the serialize_data.

    // I had a hard time figuring out how to serialize the struct before the data.
    // So I just wrote the bytes in and put a '}' at the end, and serialized
    // the values in between.
    let mut ser = serde_json::Serializer::new(
        b"{\"data\":".to_vec()
    );

    let mut seq = ser.serialize_seq(Some(df.len()))?;


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

        seq.serialize_element(&row)?;
    }

    seq.end()?;
    let mut res = String::from_utf8(ser.into_inner())?;
    res.push('}');
    Ok(res)

//    let res = json!({
//        "data": rows,
//    });
//
//    Ok(res.to_string())
}

/// Formats response `DataFrame` to JSON arrays.
fn format_jsonarrays(headers: &[String], df: DataFrame) -> Result<String, Error> {
    // use streaming serializer
    // Necessary because this way we don't create a huge vec of rows containing Value
    // (very expensive)

    // I've ended up doing this a bit more manually than I want; but I don't know how
    // to easily move between manually calling serializing methods, and having some
    // done more automatically. For example, the rows have to be serialized using
    // seq, but I can't easily call serialize_data including those rows, since I'm using
    // a manual method different from the serialize_data.

    // serialize headers
    let mut ser = serde_json::Serializer::new(
        b"{\"headers\":".to_vec()
    );
    let mut seq_headers = ser.serialize_seq(Some(headers.len()))?;

    for header in headers {
        seq_headers.serialize_element(&header)?;
    }
    seq_headers.end()?;


    // now the data
    let mut intermediate = ser.into_inner();
    intermediate.extend(b",\"data\":");


    let mut ser = serde_json::Serializer::new(intermediate);
    let mut seq_data = ser.serialize_seq(Some(df.len()))?;


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

        seq_data.serialize_element(&row)?;;
    }

    seq_data.end()?;

    // now take out vec, convert to string, and return
    let mut res = String::from_utf8(ser.into_inner())?;
    res.push('}');
    Ok(res)

//    let res = json!({
//        "headers": headers,
//        "data": rows,
//    });
}

