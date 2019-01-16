use clickhouse_rs::Pool;
use clickhouse_rs::types::Options;
use failure::Error;
use futures::future::Future;
use log::{debug, info};
use std::time::{Duration, Instant};
use tesseract_core::{Backend, DataFrame, QueryIr};

mod df;
mod sql;

use self::df::block_to_df;
use self::sql::clickhouse_sql;

// Ping timeout in millis
const PING_TIMEOUT: u64 = 100_000;

#[derive(Clone)]
pub struct Clickhouse {
    pool: Pool,
}

impl Clickhouse {
    pub fn from_addr(address: &str) -> Result<Self, Error> {
        let options = Options::new(
            address
        );

        let options = options
            // Ping timeout is necessary, because under heavy load (100 requests
            // simultenously, each one taking 5s ordinarily) the client will timeout.
            .ping_timeout(Duration::from_millis(PING_TIMEOUT));

        let pool = Pool::new(options);

        Ok(Clickhouse {
            pool,
        })
    }
}

impl Backend for Clickhouse {
    fn exec_sql(&self, sql: String) -> Box<Future<Item=DataFrame, Error=Error>> {
        let time_start = Instant::now();

        let fut = self.pool
            .get_handle()
            .and_then(|c| c.ping())
            .and_then(move |c| c.query(&sql[..]).fetch_all())
            .from_err()
            .and_then(move |(_, block)| {
                let timing = time_start.elapsed();
                info!("Time for sql execution: {}.{:03}", timing.as_secs(), timing.subsec_millis());
                //debug!("Block: {:?}", block);

                Ok(block_to_df(block)?)
            });

        Box::new(fut)
    }

    fn generate_sql(&self, query_ir: QueryIr) -> String {
        clickhouse_sql(
            &query_ir.table,
            &query_ir.cuts,
            &query_ir.drills,
            &query_ir.meas,
            &query_ir.top,
            &query_ir.sort,
            &query_ir.limit,
            &query_ir.rca,
            &query_ir.growth,
        )
    }

    // https://users.rust-lang.org/t/solved-is-it-possible-to-clone-a-boxed-trait-object/1714/4
    fn box_clone(&self) -> Box<dyn Backend + Send + Sync> {
        Box::new((*self).clone())
    }
}

