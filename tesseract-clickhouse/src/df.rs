//! Convert clickhouse Block to tesseract_core::DataFrame

use failure::{Error, bail};

use clickhouse_rs::types::{Block, ColumnType, Complex, Simple, SqlType};
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

pub trait ColumnBuilder: ColumnType {
    fn build(col_idx: usize, block: &Block<Self>) -> Result<Column, Error>;
}

macro_rules! def_column_builder {
    ( $($k:ty), * ) => {
        $(
            impl ColumnBuilder for $k {
                fn build(
                    col_idx: usize,
                    block: &Block<Self>,
                ) -> Result<Column, Error> {
                    let src_column = &block.columns()[col_idx];
                    match src_column.sql_type() {
                        SqlType::UInt8 => Ok(Column::new(
                            src_column.name().to_owned(),
                            ColumnData::UInt8(src_column.iter::<u8>()?.copied().collect()),
                        )),
                        SqlType::UInt16 => Ok(Column::new(
                            src_column.name().to_owned(),
                            ColumnData::UInt16(src_column.iter::<u16>()?.copied().collect()),
                        )),
                        SqlType::UInt32 => Ok(Column::new(
                            src_column.name().to_owned(),
                            ColumnData::UInt32(src_column.iter::<u32>()?.copied().collect()),
                        )),
                        SqlType::UInt64 => Ok(Column::new(
                            src_column.name().to_owned(),
                            ColumnData::UInt64(src_column.iter::<u64>()?.copied().collect()),
                        )),
                        SqlType::Int8 => Ok(Column::new(
                            src_column.name().to_owned(),
                            ColumnData::Int8(src_column.iter::<i8>()?.copied().collect()),
                        )),
                        SqlType::Int16 => Ok(Column::new(
                            src_column.name().to_owned(),
                            ColumnData::Int16(src_column.iter::<i16>()?.copied().collect()),
                        )),
                        SqlType::Int32 => Ok(Column::new(
                            src_column.name().to_owned(),
                            ColumnData::Int32(src_column.iter::<i32>()?.copied().collect()),
                        )),
                        SqlType::Int64 => Ok(Column::new(
                            src_column.name().to_owned(),
                            ColumnData::Int64(src_column.iter::<i64>()?.copied().collect()),
                        )),
                        SqlType::String => {
                            let mut column_data = Vec::with_capacity(block.row_count());

                            for source in src_column.iter::<&[u8]>()? {
                                let text = String::from_utf8(source.into())?;
                                column_data.push(text);
                            }

                            Ok(Column::new(
                                src_column.name().to_owned(),
                                ColumnData::Text(column_data),
                            ))
                        }
                        SqlType::Float32 => Ok(Column::new(
                            src_column.name().to_owned(),
                            ColumnData::Float32(src_column.iter::<f32>()?.copied().collect()),
                        )),
                        SqlType::Float64 => Ok(Column::new(
                            src_column.name().to_owned(),
                            ColumnData::Float64(src_column.iter::<f64>()?.copied().collect()),
                        )),
                        SqlType::Nullable(SqlType::UInt8) => Ok(Column::new(
                            src_column.name().to_owned(),
                            ColumnData::NullableUInt8(
                                src_column
                                    .iter::<Option<u8>>()?
                                    .map(|u| u.map(|v| *v))
                                    .collect(),
                            ),
                        )),
                        SqlType::Nullable(SqlType::UInt16) => Ok(Column::new(
                            src_column.name().to_owned(),
                            ColumnData::NullableUInt16(
                                src_column
                                    .iter::<Option<u16>>()?
                                    .map(|u| u.map(|v| *v))
                                    .collect(),
                            ),
                        )),
                        SqlType::Nullable(SqlType::UInt32) => Ok(Column::new(
                            src_column.name().to_owned(),
                            ColumnData::NullableUInt32(
                                src_column
                                    .iter::<Option<u32>>()?
                                    .map(|u| u.map(|v| *v))
                                    .collect(),
                            ),
                        )),
                        SqlType::Nullable(SqlType::UInt64) => Ok(Column::new(
                            src_column.name().to_owned(),
                            ColumnData::NullableUInt64(
                                src_column
                                    .iter::<Option<u64>>()?
                                    .map(|u| u.map(|v| *v))
                                    .collect(),
                            ),
                        )),
                        SqlType::Nullable(SqlType::Int8) => Ok(Column::new(
                            src_column.name().to_owned(),
                            ColumnData::NullableInt8(
                                src_column
                                    .iter::<Option<i8>>()?
                                    .map(|u| u.map(|v| *v))
                                    .collect(),
                            ),
                        )),
                        SqlType::Nullable(SqlType::Int16) => Ok(Column::new(
                            src_column.name().to_owned(),
                            ColumnData::NullableInt16(
                                src_column
                                    .iter::<Option<i16>>()?
                                    .map(|u| u.map(|v| *v))
                                    .collect(),
                            ),
                        )),
                        SqlType::Nullable(SqlType::Int32) => Ok(Column::new(
                            src_column.name().to_owned(),
                            ColumnData::NullableInt32(
                                src_column
                                    .iter::<Option<i32>>()?
                                    .map(|u| u.map(|v| *v))
                                    .collect(),
                            ),
                        )),
                        SqlType::Nullable(SqlType::Int64) => Ok(Column::new(
                            src_column.name().to_owned(),
                            ColumnData::NullableInt64(
                                src_column
                                    .iter::<Option<i64>>()?
                                    .map(|u| u.map(|v| *v))
                                    .collect(),
                            ),
                        )),
                        SqlType::Nullable(SqlType::String) => {
                            let mut column_data = Vec::with_capacity(block.row_count());

                            for source in src_column.iter::<Option<&[u8]>>()? {
                                let text = match source {
                                    None => None,
                                    Some(source) => Some(String::from_utf8(source.into())?),
                                };
                                column_data.push(text);
                            }

                            Ok(Column::new(
                                src_column.name().to_owned(),
                                ColumnData::NullableText(column_data),
                            ))
                        }
                        SqlType::Nullable(SqlType::Float32) => Ok(Column::new(
                            src_column.name().to_owned(),
                            ColumnData::NullableFloat32(
                                src_column
                                    .iter::<Option<f32>>()?
                                    .map(|u| u.map(|v| *v))
                                    .collect(),
                            ),
                        )),
                        SqlType::Nullable(SqlType::Float64) => Ok(Column::new(
                            src_column.name().to_owned(),
                            ColumnData::NullableFloat64(
                                src_column
                                    .iter::<Option<f64>>()?
                                    .map(|u| u.map(|v| *v))
                                    .collect(),
                            ),
                        )),
                        s => bail!("{} is not supported by tesseract", s),
                    }
                }
            }
        )*
    };
}

def_column_builder! {
    Simple,
    Complex
}

pub fn block_to_df<K: ColumnBuilder>(block: Block<K>) -> Result<DataFrame, Error> {
    let mut df = Vec::with_capacity(block.column_count());

    for (index, col) in block.columns().iter().enumerate() {
        df.push(K::build(index, &block)?);
    }

    Ok(DataFrame::from_vec(df))
}
