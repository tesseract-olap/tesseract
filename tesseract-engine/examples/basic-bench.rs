/// This tests backend query only
///
/// Mostly to get an idea of performance

extern crate failure;
extern crate tesseract_engine;

use failure::Error;
use std::fs;
use std::time;
use tesseract_engine::{
    backends::columnar::ColumnarTable,
    query::BackendQuery,
};

fn main() -> Result<(), Error> {
    let test_schema_path = "test-data.sql";
    let test_data_path = "test-data.csv";

    let test_schema = fs::read_to_string(test_schema_path)?;
    let mut table = ColumnarTable::create(&test_schema).unwrap();
    println!("{:?}\n", table);

    let now = time::Instant::now();
    let test_data = fs::read_to_string(test_data_path)?;
    table.import_csv(&test_data).unwrap();
    let end = now.elapsed();
    println!("import csv: {}.{}", end.as_secs(), end.subsec_millis());

    let now = time::Instant::now();
    let res = table.execute_query(&BackendQuery {
        drilldowns: vec!["year".to_owned(), "age".to_owned()],
        measures: vec!["population".to_owned()],
    }).unwrap();
    let end = now.elapsed();

    println!("{}", res);
    println!("execute query: {}.{}", end.as_secs(), end.subsec_millis());

    Ok(())
}
