use clickhouse_rs::{Client, Options};
use failure::Error;
use futures::future::Future;
use log::{debug, info};
use std::time::Instant;
use tesseract_core::{Backend, DataFrame};

mod df;

use self::df::block_to_df;

#[derive(Clone)]
pub struct Clickhouse {
    options: Options
}

impl Clickhouse {
    pub fn from_addr(address: &str) -> Result<Self, Error> {
        let options = Options::new(
            address
                .parse()
                .expect("Could not parse Clickhouse db url")
        );

        Ok(Clickhouse {
            options,
        })
    }
}

impl Backend for Clickhouse {
    fn exec_sql(&self, sql: String) -> Box<Future<Item=DataFrame, Error=Error>> {
        let time_start = Instant::now();

        let fut = Client::connect(self.options.clone())
            .and_then(|c| c.ping())
            .and_then(move |c| c.query_all(&sql[..]))
            .from_err()
            .and_then(move |(_, block)| {
                let timing = time_start.elapsed();
                info!("Time for sql execution: {}.{}", timing.as_secs(), timing.subsec_millis());
                debug!("Block: {:?}", block);

                Ok(block_to_df(block)?)
            });

        Box::new(fut)
    }

    // https://users.rust-lang.org/t/solved-is-it-possible-to-clone-a-boxed-trait-object/1714/4
    fn box_clone(&self) -> Box<dyn Backend + Send + Sync> {
        Box::new((*self).clone())
    }
}

