#![feature(try_from)]
//! tesseract-core contains Schema;
//! Schema is stateless; it is constructed from the schema file.
//! Schema is held in the AppState struct to provide access from a route
//!
//! Each route instance will apply a tesseract_core::Query to tesseract_core::Schema to get sql.
//! The route instance then sends sql to database and gets results back in a
//! tesseract_core::Dataframe
//!
//! Dataframe is then applied to Schema to format result. (for now, jsonrecords only)
//!
//!
//! Backend trait: exec() takes in a sql string, outputs a dataframe.
//! Because tesseract-core generates just sql (instead of taking a query and schema into a
//! `Backend`, it allows different kinds of backends to be used. Don't have to worry about
//! async/sync, which is the hardest difference to manage. Otherwise it would be easy to define
//! a `Backend` trait. (Or, can I do this and define the Backend trait as defining futures,
//! and have sync operations return a future? This might still be tricky dealing with actix,
//! it's overall easier to leave backend handling on the server side entirely).
//!
//! The database is able to be declared in the schema, each fact table and dim can be from
//! different databases. Supported: clickhouse, postgres, mysql, sqlite.

mod app;
mod handlers;

use actix_web::server;
use dotenv::dotenv;
use failure::{Error, format_err};
use std::env;
use structopt::StructOpt;

use tesseract_clickhouse::Clickhouse;
use tesseract_core::Schema;

fn main() -> Result<(), Error> {
    // Configuration

    pretty_env_logger::init();
    dotenv().ok();
    let opt = Opt::from_args();

    let server_addr = opt.address.unwrap_or("127.0.0.1:7777".to_owned());
    let schema_path = env::var("TESSERACT_SCHEMA_FILEPATH")
        .expect("TESSERACT_SCHEMA_FILEPATH not found");

    // DB options: For now, only one db at a time, and only
    // clickhouse or mysql
    // They're set to conflict with each other in cli opts
    //
    // Also, casting to trait object:
    // https://stackoverflow.com/questions/38294911/how-do-i-cast-a-literal-value-to-a-trait-object
    //
    // Also, it needs to be safe to send between threads, so add trait bounds
    // Send + Sync.
    // https://users.rust-lang.org/t/sending-trait-objects-between-threads/2374
    //
    // Also, it needs to be clonable to move into the closure that is
    // used to initialize actix-web, so there's a litle boilerplate
    // to implement https://users.rust-lang.org/t/solved-is-it-possible-to-clone-a-boxed-trait-object/1714/4
    let mut db_url = String::new();

    let clickhouse_db = env::var("CLICKHOUSE_DATABASE_URL")
        .or(opt.clickhouse_db_url.ok_or(format_err!("")))
        .and_then(|url| {
            let db = Clickhouse::from_addr(&url);
            db_url = url;
            db
        })
        .map(|db| Box::new(db) as Box<dyn Backend + Send + Sync>);

    let mysql_db = env::var("MYSQL_DATABASE_URL")
        .or(opt.mysql_db_url.ok_or(format_err!("")))
        .and_then(|url| {
            // TODO replace with mysql backend here
            let db = MySql::from_addr(&url);
            db_url = url;
            db
        })
        .map(|db| Box::new(db) as Box<dyn Backend + Send + Sync>);

    let db = clickhouse_db.or(mysql_db)
        .expect("No database url found");

    // Initialize Schema
    let schema_str = std::fs::read_to_string(&schema_path)
        .map_err(|_| format_err!("Schema file not found at {}", schema_path))?;
    let schema = Schema::from_json(&schema_str)?;

    // Initialize Server
    let sys = actix::System::new("tesseract");
    server::new(move|| app::create_app(db.clone(), schema.clone()))
        .bind(&server_addr)
        .expect(&format!("cannot bind to {}", server_addr))
        .start();

    println!("Tesseract listening on: {}", server_addr);
    println!("Tesseract database:   {}", db_url);
    println!("Tesseract schema path:  {}", schema_path);

    sys.run();

    Ok(())
}

#[derive(Debug, StructOpt)]
#[structopt(name="tesseract")]
struct Opt {
    #[structopt(short="a", long="addr")]
    address: Option<String>,

    #[structopt(long="clickhouse-url")]
    #[structopt(conflicts_with="mysql-url")]
    clickhouse_db_url: Option<String>,

    #[structopt(long="mysql-url")]
    mysql_db_url: Option<String>,
}

#[derive(Debug, Clone)]
struct EnvVars {
    pub flush_secret: Option<String>,
    pub database_url: String,
    pub schema_filepath: Option<String>,
}

// TODO delete below:
// This is just for testing the trait object

use tesseract_core::{Backend, DataFrame};
use futures::future::Future;

#[derive(Clone)]
pub struct MySql {}

impl MySql {
    fn from_addr(s: &str) -> Result<Self, Error> {
        Ok(MySql{})
    }
}

impl Backend for MySql {
    fn exec_sql(&self, sql: String) -> Box<Future<Item=DataFrame, Error=Error>>
    {
        Box::new(
            futures::future::result(
                Ok(DataFrame::new())
            )
        )
    }

    fn box_clone(&self) -> Box<dyn Backend + Send + Sync> {
        Box::new((*self).clone())
    }
}
