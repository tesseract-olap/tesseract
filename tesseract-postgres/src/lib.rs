use failure::{Error, format_err};
use tesseract_core::{Backend, DataFrame};
use futures::{Future, Stream};
use tokio_postgres::NoTls;
extern crate futures;
extern crate tokio_postgres;

mod df;
use self::df::{rows_to_df};


#[derive(Clone)]
pub struct Postgres {
    db_url: String
}

impl Postgres {
    pub fn new(address: &str) -> Postgres {

        Postgres { db_url: address.to_string() }
    }

    pub fn from_addr(address: &str) -> Result<Self, Error> {
        Ok(Postgres::new(address))
    }

    pub fn hangup() {
        println!("Done with connection! TODO!");
    }
}

// TODO:
// 1. better connection lifecycle management!
// 2. dataframe creation

impl Backend for Postgres {
    fn exec_sql(&self, sql: String) -> Box<Future<Item=DataFrame, Error=Error>> {
        let future = tokio_postgres::connect(&self.db_url, NoTls)
            .and_then(move |(mut client, connection)|{
                let connection = connection.map_err(|e| eprintln!("connection error: {}", e));
                tokio::spawn(connection);
                client.prepare(&sql).map(|statement| (client, statement))
            })
            .map_err(|err| format_err!("psql err {}", err))
            .and_then(|(mut client, statement)| {
                let rows_vec = client.query(&statement, &[]).collect();
                rows_to_df(rows_vec, statement.columns())
            });
        Box::new(future)
    }

    fn box_clone(&self) -> Box<dyn Backend + Send + Sync> {
        Box::new((*self).clone())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use tokio::runtime::current_thread::Runtime;
    use tesseract_core::{ColumnData};
    #[test]
    fn test_pg_query() {
        let postgres_db= env::var("TESSERACT_DATABASE_URL").expect("Please provide TESSERACT_DATABASE_URL");
        let pg = Postgres::new(&postgres_db);
        let future = pg.exec_sql("SELECT 1337 as hello;".to_string()).map(|df| {
            println!("Result was: {:?}", df);
            let expected_len: usize = 1;
            let val = match df.columns[0].column_data {
                ColumnData::Int32(ref internal_data) => internal_data[0],
                _ => -1
            };
            assert_eq!(df.len(), expected_len);
            assert_eq!(val, 1337);
            })
            .map_err(|err| {
               println!("Got error {:?}", err);
                ()
            });

        let mut rt = Runtime::new().unwrap();
        rt.block_on(future).unwrap();
    }
}
