extern crate clickhouse_rs;
extern crate tokio;

// Basic testing structured taken from
// clickhouse-rs database tests by suharev7.
use tokio::prelude::*;
use std::{
    env
};
use clickhouse_rs::{
    errors::Error, types::Block, types::Decimal, types::FromSql, ClientHandle, Pool,
};

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