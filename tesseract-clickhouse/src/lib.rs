use clickhouse_rs::Pool;
use clickhouse_rs::types::{Options, Simple, Complex, Block};
use failure::{Error, format_err};
use futures::{future, Future, Stream};
use log::*;
use std::time::{Duration, Instant};
use tesseract_core::{Backend, DataFrame, QueryIr};
use tesseract_core::schema::metadata::SchemaPhysicalData;

mod df;
mod sql;

use self::df::{block_to_df};
use self::sql::clickhouse_sql;

// Ping timeout in millis
const PING_TIMEOUT: u64 = 100_000;

#[derive(Clone)]
pub struct Clickhouse {
    pool: Pool,
}

impl Clickhouse {
    pub fn from_url(url: &str) -> Result<Self, Error> {
        let options = format!("tcp://{}", url).parse::<Options>()?;

        let options = options
            // Ping timeout is necessary, because under heavy load (100 requests
            // simultaneously, each one taking 5s ordinarily) the client will timeout.
            .ping_timeout(Duration::from_millis(PING_TIMEOUT));

        let pool = Pool::new(options);

        Ok(Clickhouse {
            pool,
        })
    }
}

impl Backend for Clickhouse {
    fn exec_sql(&self, sql: String) -> Box<dyn Future<Item=DataFrame, Error=Error>> {
        let time_start = Instant::now();

        let fut = self.pool
            .get_handle()
            .and_then(move |c| c.query(&sql[..]).fetch_all())
            .from_err()
            .and_then(move |(_, block): (_, Block<Complex>)| {
                let timing = time_start.elapsed();
                info!("Time for sql execution: {}.{:03}", timing.as_secs(), timing.subsec_millis());
                //debug!("Block: {:?}", block);

                Ok(block_to_df(block)?)
            });

        Box::new(fut)
    }

    fn exec_sql_stream(&self, sql: String) -> Box<dyn Stream<Item=Result<DataFrame, Error>, Error=Error>> {
        let fut_stream = self.pool
            .get_handle()
            .and_then(move |c| {
                future::ok(
                    c.query(&sql[..])
                        .stream_blocks()
                        .map(move |block: Block<Simple>| {
                            block_to_df(block)
                        })
                )
            })
            .flatten_stream()
            .map_err(|err| format_err!("{}", err));

        Box::new(fut_stream)
    }

    // https://users.rust-lang.org/t/solved-is-it-possible-to-clone-a-boxed-trait-object/1714/4
    fn box_clone(&self) -> Box<dyn Backend + Send + Sync> {
        Box::new((*self).clone())
    }

    fn generate_sql(&self, query_ir: QueryIr) -> String {
        clickhouse_sql(
            &query_ir
        )
    }

    fn retrieve_schemas(&self, tablepath: &str, id: Option<&str>) -> Box<dyn Future<Item=Vec<SchemaPhysicalData>, Error=Error>> {
        let sql = match id {
            None => format!("SELECT id, schema FROM {}", tablepath),
            Some(id_val) => format!("SELECT id, schema FROM {} WHERE id = '{}'", tablepath, id_val),
        };
        let time_start = Instant::now();
        let fut = self.pool
            .get_handle()
            .and_then(move |c| c.query(&sql).fetch_all())
            .from_err()
            .and_then(move |(_, block): (_, Block<Complex>)| {
                let timing = time_start.elapsed();
                info!("Time for sql schema retrieval: {}.{:03}", timing.as_secs(), timing.subsec_millis());
                let schema_vec: Vec<SchemaPhysicalData> = block.rows().map(|row| {
                    SchemaPhysicalData {
                        id: row.get("id").expect("missing id"),
                        content: row.get("schema").expect("missing schema"),
                        format: "json".to_string(),
                    }
                }).collect();
                Ok(schema_vec)
            });

        Box::new(fut)
    }

    fn update_schema(&self, tablepath: &str, schema_name_id: &str, schema_content: &str) -> Box<dyn Future<Item=bool, Error=Error>> {
        let sql = format!("ALTER TABLE {} UPDATE schema = '{}' WHERE id = '{}'", tablepath, schema_content, schema_name_id);
        let time_start = Instant::now();
        let fut = self.pool
            .get_handle()
            .and_then(move |c| {
                c.execute(sql)
            })
            .and_then(move |c| {
                let timing = time_start.elapsed();
                info!("Time for updating schema: {}.{:03}", timing.as_secs(), timing.subsec_millis());
                Ok(true)
            })
            .from_err();

        Box::new(fut)
    }


    fn add_schema(&self, tablepath: &str, schema_name_id: &str, content: &str) -> Box<dyn Future<Item=bool, Error=Error>> {
        let block = Block::new()
            .column("id", vec![schema_name_id])
            .column("schema", vec![content]);
        let table_name_copy = tablepath.to_string();
        let time_start = Instant::now();
        let fut = self.pool
            .get_handle()
            .and_then(move |c| {
                c.insert(table_name_copy, block)
            })
            .and_then(move |c| {
                let timing = time_start.elapsed();
                info!("Time for adding schema: {}.{:03}", timing.as_secs(), timing.subsec_millis());
                Ok(true)
            })
            .from_err();

        Box::new(fut)
    }

    fn delete_schema(&self, tablepath: &str, schema_name_id: &str) -> Box<dyn Future<Item=bool, Error=Error>> {
        let sql = format!("ALTER TABLE {} DELETE WHERE id = '{}'", tablepath, schema_name_id);
        let time_start = Instant::now();
        let fut = self.pool
            .get_handle()
            .and_then(move |c| c.execute(sql))
            .and_then(move |c| {
                let timing = time_start.elapsed();
                info!("Time for deleting schema: {}.{:03}", timing.as_secs(), timing.subsec_millis());
                Ok(true)
            })
            .from_err();

        Box::new(fut)
    }
}

