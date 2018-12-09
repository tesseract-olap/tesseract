use failure::Error;
use futures::future::Future;
use crate::dataframe::DataFrame;

pub trait Backend {
    fn exec_sql(&self, sql: String) -> Box<Future<Item=DataFrame, Error=Error>>;
    fn box_clone(&self) -> Box<dyn Backend + Send + Sync>;
}

impl Clone for Box<dyn Backend + Send + Sync> {
    fn clone(&self) -> Box<Backend + Send + Sync> {
        self.box_clone()
    }
}
