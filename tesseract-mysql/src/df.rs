//! Convert clickhouse Block to tesseract_core::DataFrame
extern crate mysql;
use failure::{Error, bail};
use mysql::QueryResult;
use mysql::consts::ColumnType::*;
use tesseract_core::{DataFrame, Column, ColumnData};

// was able to replace this with done()
// pub fn query_future_wrapper(query_result: QueryResult) -> impl Future<Item = DataFrame, Error = Error> {
//     let res: DataFrame = self::queryresult_to_df(query_result).expect("Failed to build dataframe");
//     ok(res)
// }

pub fn queryresult_to_df(query_result: QueryResult) -> Result<DataFrame, Error> {
    let mut tcolumn_list = vec![];
    let columns = query_result.columns_ref();

    // for each column figure out my type. add it to a vec
    for col in columns.iter() {
        let col_type = col.column_type();
        let col_name = col.name_str();
        // println!("NAME: {:?} TYPE {:?}", col.name_str(), col_type);
        match col_type {
            MYSQL_TYPE_TINY => {
                tcolumn_list.push(Column::new(
                    col_name.to_string(),
                    ColumnData::Int8(vec![]),
                ))
            },
            MYSQL_TYPE_SHORT => {
                tcolumn_list.push(Column::new(
                    col_name.to_string(),
                    ColumnData::Int16(vec![]),
                ))
            },
            // confusing but TYPE_LONG is regular integer (32-bit)
            // see https://dev.mysql.com/doc/refman/8.0/en/c-api-prepared-statement-type-codes.html
            MYSQL_TYPE_LONG => {
                tcolumn_list.push(Column::new(
                    col_name.to_string(),
                    ColumnData::Int32(vec![]),
                ))
            },
            MYSQL_TYPE_LONGLONG => {
                tcolumn_list.push(Column::new(
                    col_name.to_string(),
                    ColumnData::Int64(vec![]),
                ))
            },
            MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING=> {
                tcolumn_list.push(Column::new(
                    col_name.to_string(),
                    ColumnData::Text(vec![]),
                ))
            },
            MYSQL_TYPE_FLOAT => {
                tcolumn_list.push(Column::new(
                    col_name.to_string(),
                    ColumnData::Float32(vec![]),
                ))
            },
            MYSQL_TYPE_DOUBLE => {
                tcolumn_list.push(Column::new(
                    col_name.to_string(),
                    ColumnData::Float64(vec![]),
                ))
            },
            s => bail!("mysql sql type not supported: {:?}", s),
        }
    }

    query_result.for_each(|x| {
        let row = x.unwrap();
        // for (col_idx, mut col) in tcolumn_list.iter().enumerate() {
        for col_idx in 0..tcolumn_list.len() {
            let column_data = tcolumn_list
                .get_mut(col_idx)
                .expect("logic checked?")
                .column_data();
            match column_data {
                ColumnData::UInt64(col_data) => {
                    col_data.push(row.get(col_idx).expect("Data unpacking failure"));
                },
                ColumnData::UInt32(col_data) => {
                    col_data.push(row.get(col_idx).expect("Data unpacking failure"));
                },
                ColumnData::UInt16(col_data) => {
                    col_data.push(row.get(col_idx).expect("Data unpacking failure"));
                },
                ColumnData::UInt8(col_data) => {
                    col_data.push(row.get(col_idx).expect("Data unpacking failure"));
                },
                ColumnData::Int64(col_data) => {
                    col_data.push(row.get(col_idx).expect("Data unpacking failure"));
                },
                ColumnData::Int32(col_data) => {
                    col_data.push(row.get(col_idx).expect("Data unpacking failure"));
                },
                ColumnData::Int16(col_data) => {
                    col_data.push(row.get(col_idx).expect("Data unpacking failure"));
                },
                ColumnData::Int8(col_data) => {
                    col_data.push(row.get(col_idx).expect("Data unpacking failure"));
                },
                ColumnData::Float32(col_data) => {
                    col_data.push(row.get(col_idx).expect("Data unpacking failure"));
                },
                ColumnData::Float64(col_data) => {
                    col_data.push(row.get(col_idx).expect("Data unpacking failure"));
                },
                ColumnData::Text(col_data) => {
                    col_data.push(row.get(col_idx).expect("Data unpacking failure"));
                },
            }
        }
    });

    let df = DataFrame::from_vec(tcolumn_list);
    println!("WOOT WOOT ! {:?}", df);
    Ok(df)
}

