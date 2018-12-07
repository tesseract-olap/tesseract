use failure::Error;
use futures::future::Future;
use crate::dataframe::DataFrame;

pub trait Backend {
    fn exec_sql(&self, sql: String) -> Box<Future<Item=DataFrame, Error=Error>>;
}
