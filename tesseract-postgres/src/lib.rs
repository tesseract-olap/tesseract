use failure::{Error, format_err};
use tesseract_core::{Backend, DataFrame};
use futures::{Future, Stream};
use tokio_postgres::NoTls;

extern crate futures;
extern crate tokio_postgres;


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
}

impl Backend for Postgres {
    fn exec_sql(&self, sql: String) -> Box<Future<Item=DataFrame, Error=Error>> {
        let future = tokio_postgres::connect(&self.db_url, NoTls)
            .and_then(move |(mut client, connection)|{
                let connection = connection.map_err(|e| eprintln!("connection error: {}", e));
                tokio::spawn(connection);
                client.prepare(&sql).map(|statement| (client, statement))
            })
            .and_then(|(mut client, statement)| {
                client.query(&statement, &[]).collect()
            })
            .map_err(|err| format_err!("psql err {}", err))
            .map(|rows| {
                let r = rows[0].get::<_, i32>(0);
                println!("{:?}", r);
                assert_eq!(3, r);
                DataFrame::new()
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

    #[test]
    fn test_pg_query() {
        let postgres_db= env::var("TESSERACT_DATABASE_URL").expect("Please provide TESSERACT_DATABASE_URL");
        let pg = Postgres::new(&postgres_db);
        let x = pg.exec_sql("SELECT 1+3".to_string());
        // WHY DOES THIS NOT WORK?
        // tokio::run(x);
    }
}
