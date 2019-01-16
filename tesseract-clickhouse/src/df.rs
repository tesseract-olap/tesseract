//! Convert clickhouse Block to tesseract_core::DataFrame

use failure::{Error, bail};

use clickhouse_rs::types::{Block, SqlType};
use tesseract_core::{DataFrame, Column, ColumnData};

// from source code of clickhouse_rs
//             SqlType::UInt8 => "UInt8".into(),
//            SqlType::UInt16 => "UInt16".into(),
//            SqlType::UInt32 => "UInt32".into(),
//            SqlType::UInt64 => "UInt64".into(),
//            SqlType::Int8 => "Int8".into(),
//            SqlType::Int16 => "Int16".into(),
//            SqlType::Int32 => "Int32".into(),
//            SqlType::Int64 => "Int64".into(),
//            SqlType::String => "String".into(),
//            SqlType::Float32 => "Float32".into(),
//            SqlType::Float64 => "Float64".into(),
//            SqlType::Date => "Date".into(),
//            SqlType::DateTime => "DateTime".into(),

pub fn block_to_df(block: Block) -> Result<DataFrame, Error> {
    let mut df = vec![];

    for col in block.columns() {
        match col.sql_type() {
            SqlType::UInt8 => {
                df.push(Column::new(
                    col.name().to_owned(),
                    ColumnData::UInt8(vec![]),
                ))
            },
            SqlType::UInt16 => {
                df.push(Column::new(
                    col.name().to_owned(),
                    ColumnData::UInt16(vec![]),
                ))
            },
            SqlType::UInt32 => {
                df.push(Column::new(
                    col.name().to_owned(),
                    ColumnData::UInt32(vec![]),
                ))
            },
            SqlType::UInt64 => {
                df.push(Column::new(
                    col.name().to_owned(),
                    ColumnData::UInt64(vec![]),
                ))
            },
            SqlType::Int8 => {
                df.push(Column::new(
                    col.name().to_owned(),
                    ColumnData::Int8(vec![]),
                ))
            },
            SqlType::Int16 => {
                df.push(Column::new(
                    col.name().to_owned(),
                    ColumnData::Int16(vec![]),
                ))
            },
            SqlType::Int32 => {
                df.push(Column::new(
                    col.name().to_owned(),
                    ColumnData::Int32(vec![]),
                ))
            },
            SqlType::Int64 => {
                df.push(Column::new(
                    col.name().to_owned(),
                    ColumnData::Int64(vec![]),
                ))
            },
            SqlType::String => {
                df.push(Column::new(
                    col.name().to_owned(),
                    ColumnData::Text(vec![]),
                ))
            },
            SqlType::Float32 => {
                df.push(Column::new(
                    col.name().to_owned(),
                    ColumnData::Float32(vec![]),
                ))
            },
            SqlType::Float64 => {
                df.push(Column::new(
                    col.name().to_owned(),
                    ColumnData::Float64(vec![]),
                ))
            },
            s => bail!("{} is not supported by tesseract", s),
        }
    }

    for row_idx in 0..block.row_count() {
        for col_idx in 0..block.column_count() {
            let column_data = df
                .get_mut(col_idx)
                .expect("logic checked?")
                .column_data();

            match column_data {
                ColumnData::UInt8(col_data) => {
                    col_data.push(block.get(row_idx, col_idx)?);
                },
                ColumnData::UInt16(col_data) => {
                    col_data.push(block.get(row_idx, col_idx)?);
                },
                ColumnData::UInt32(col_data) => {
                    col_data.push(block.get(row_idx, col_idx)?);
                },
                ColumnData::UInt64(col_data) => {
                    col_data.push(block.get(row_idx, col_idx)?);
                },
                ColumnData::Int8(col_data) => {
                    col_data.push(block.get(row_idx, col_idx)?);
                },
                ColumnData::Int16(col_data) => {
                    col_data.push(block.get(row_idx, col_idx)?);
                },
                ColumnData::Int32(col_data) => {
                    col_data.push(block.get(row_idx, col_idx)?);
                },
                ColumnData::Int64(col_data) => {
                    col_data.push(block.get(row_idx, col_idx)?);
                },
                ColumnData::Text(col_data) => {
                    col_data.push(block.get(row_idx, col_idx)?);
                },
                ColumnData::Float32(col_data) => {
                    col_data.push(block.get(row_idx, col_idx)?);
                },
                ColumnData::Float64(col_data) => {
                    col_data.push(block.get(row_idx, col_idx)?);
                },
            }
        }
    }

    Ok(DataFrame::from_vec(df))
}

