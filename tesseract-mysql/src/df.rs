//! Convert clickhouse Block to tesseract_core::DataFrame
// extern crate mysql;
use failure::{Error, bail};
extern crate mysql_async;
extern crate futures;
use futures::future::ok;
use futures::future::{FutureResult};
use mysql_async::{QueryResult, BinaryProtocol, Conn};
use mysql_async::consts::ColumnType::*;
use tesseract_core::{DataFrame, Column, ColumnData};
use mysql_async::Value::*;
// use mysql_async::{Row};
use mysql_async::futures::{ForEach};

pub fn build_column_vec(query_result: &QueryResult<Conn, BinaryProtocol>) -> Result<Vec<Column>, Error> {
    let mut tcolumn_list = vec![];
    let columns = query_result.columns_ref();

    // for each column figure out my type. add it to a vec
    for col in columns.iter() {
        let col_type = col.column_type();
        let col_name = col.name_str();
        // println!("NAME: {:?} TYPE {:?}", col.name_str(), col_type);
        match col_type {
            // confusing but TYPE_LONG is regular integer (32-bit)
            // see https://dev.mysql.com/doc/refman/8.0/en/c-api-prepared-statement-type-codes.html
            MYSQL_TYPE_LONGLONG | MYSQL_TYPE_LONG | MYSQL_TYPE_SHORT | MYSQL_TYPE_TINY => {
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
            unknown => bail!("MySQL type not supported: {:?}", unknown),
        }
    }
    Ok(tcolumn_list)
}


pub fn push_data_to_vec(mut tcolumn_list: Vec<Column>, query_result: QueryResult<Conn, BinaryProtocol>) -> futures::future::Either<FutureResult<QueryResult<Conn, BinaryProtocol>, my::errors::Error>, ForEach<Conn, BinaryProtocol, u32>> {
    let future = query_result.for_each(|x| {
        let row = x.unwrap();
        println!("Arrived to this point...");
        for col_idx in 0..tcolumn_list.len() {
            let column_data = tcolumn_list
                .get_mut(col_idx)
                .expect("logic checked?")
                .column_data();
            match column_data {
                ColumnData::Int64(col_data) => {
                    let raw_value = row.get(col_idx).unwrap();
                    match raw_value {
                        Int(y) => Some(col_data.push(*y)),
                        s => {
                            println!("No match for {:?}", s);
                            None
                        }
                    };
                },
                s => {
                    println!("FAILING HERE!");
                }
            }
        }
    });
    future
    // an alternative approach of simply calling future.wait() 
    // then building the dataframe works, but defeats the power of the async
    // future.wait();
    // DataFrame::from_vec(tcolumn_list)
}
