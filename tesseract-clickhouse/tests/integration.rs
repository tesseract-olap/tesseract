extern crate clickhouse_rs;
extern crate tokio;

// Basic testing structured taken from
// clickhouse-rs database tests by suharev7.
use tokio::prelude::*;
use std::{
    env
};
use clickhouse_rs::{
    types::Block, ClientHandle, Pool,
};
use clickhouse_rs::types::Complex;
// use failure::{Error, format_err};

fn database_url() -> String {
    let tmp = env::var("TESSERACT_DATABASE_URL").unwrap_or_else(|_| "tcp://localhost:9000?compression=lz4".into());
    tmp.replace("clickhouse://", "tcp://")
}

fn run<F, T, U>(future: F) -> Result<T, U>
where
    F: Future<Item = T, Error = U> + Send + 'static,
    T: Send + 'static,
    U: Send + 'static,
{
    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    let result = runtime.block_on(future);
    runtime.shutdown_on_idle().wait().unwrap();
    result
}

#[test]
fn test_ping() {
    // This test is meant as a sanity check
    // to ensure the docker provisioning process worked
    let pool = Pool::new(database_url());
    let done = pool.get_handle().and_then(ClientHandle::ping).map(|_| ());
    run(done).unwrap()
}

#[test]
fn test_query() {
    #[derive(Debug, Clone, PartialEq)]
    pub struct RowResult {
        pub id_count: i64,
    }

    // This test is meant as a sanity check
    // to ensure the SQL ingestion worked
    let pool = Pool::new(database_url());
    let sql = "SELECT COUNT(*) as id_count FROM tesseract_webshop_time;";
    let fut = pool.get_handle()
        .and_then(move |c| {
            c.query(&sql).fetch_all()
        })
        .and_then(move |(_, block): (_, Block<Complex>)| {
            let schema_vec: Vec<RowResult> = block.rows().map(|row| {
                RowResult {
                    id_count: row.get("id_count").expect("missing id_count"),
                }
            }).collect();
            Ok(schema_vec)
        });

    let res = run(fut).unwrap();
    println!("RES={:?}", res);
    // assert!(res.get(0).unwrap().id_count > 0);
}