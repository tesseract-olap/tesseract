use csv;
use failure::Error;

use crate::dataframe::{DataFrame, ColumnData};

pub fn format_csv(headers: &[String], df: DataFrame) -> Result<String, Error> {
    let mut wtr = csv::WriterBuilder::new()
        .from_writer(vec![]);

    // write header
    wtr.write_record(headers)?;

    let mut row_buf = vec![];

    // write data
    for row_idx in 0..df.len() {
        for col_idx in 0..df.columns.len() {
            let val = match df.columns[col_idx].column_data {
                ColumnData::Int8(ref ns) => ns[row_idx].to_string(),
                ColumnData::Int16(ref ns) => ns[row_idx].to_string(),
                ColumnData::Int32(ref ns) => ns[row_idx].to_string(),
                ColumnData::Int64(ref ns) => ns[row_idx].to_string(),
                ColumnData::UInt8(ref ns) => ns[row_idx].to_string(),
                ColumnData::UInt16(ref ns) => ns[row_idx].to_string(),
                ColumnData::UInt32(ref ns) => ns[row_idx].to_string(),
                ColumnData::UInt64(ref ns) => ns[row_idx].to_string(),
                ColumnData::Float32(ref ns) => ns[row_idx].to_string(),
                ColumnData::Float64(ref ns) => ns[row_idx].to_string(),
                ColumnData::Text(ref ss) => ss[row_idx].to_string(),
            };

            row_buf.push(val);
        }
        wtr.write_record(&row_buf)?;

        row_buf.clear();
    }

    let res = String::from_utf8(wtr.into_inner()?)?;

    Ok(res)
}
