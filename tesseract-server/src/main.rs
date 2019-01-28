#![feature(try_from)]
#![feature(transpose_result)]

//! tesseract-core contains Schema;
//! Schema is stateless; it is constructed from the schema file.
//! Schema is held in the AppState struct to provide access from a route
//!
//! Each route instance will apply a tesseract_core::Query to tesseract_core::Schema to get sql.
//! The route instance then sends sql to a database and gets results back in a
//! tesseract_core::DataFrame. DataFrame is then applied to Schema to format result (jsonrecords
//! or csv).
//!
//!
//! Backend trait: exec_sql() takes in a sql string, outputs a DataFrame.
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
mod db_config;
mod handlers;
mod logic_layer;
mod schema_config;

use actix_web::server;
use dotenv::dotenv;
use failure::{Error, format_err};
use std::env;
use structopt::StructOpt;

use std::sync::{Arc, RwLock};

use crate::app::{EnvVars, SchemaSource, create_app};


fn main() -> Result<(), Error> {
    // Configuration

    pretty_env_logger::init();
    dotenv().ok();
    let opt = Opt::from_args();

    let server_addr = opt.address.unwrap_or("127.0.0.1:7777".to_owned());

    let flush_secret = env::var("TESSERACT_FLUSH_SECRET").ok();

    // Database
    let db_url_full = env::var("TESSERACT_DATABASE_URL")
        .or(opt.database_url.ok_or(format_err!("")))
        .map_err(|_| format_err!("database url not found; either TESSERACT_DATABASE_URL or cli option required"))?;

    let (db, db_url, db_type) = db_config::get_db(&db_url_full)?;
    let db_type_viz = db_type.clone();

    // Schema
    let schema_path = env::var("TESSERACT_SCHEMA_FILEPATH")
        .expect("TESSERACT_SCHEMA_FILEPATH not found");

    // NOTE: Local schema is the only supported SchemaSource for now
    let schema_source = SchemaSource::LocalSchema { filepath: schema_path.clone() };

    let schema = schema_config::read_schema(&schema_path).unwrap_or_else(|err| {
        panic!(err);
    });
    let schema_arc = Arc::new(RwLock::new(schema.clone()));

    // Env
    let env_vars = EnvVars {
        database_url: db_url.clone(),
        schema_source,
        flush_secret,
    };

    // TODO: Populate internal cache
    let cache = logic_layer::populate_cache(schema.clone());

    // Initialize Server
    let sys = actix::System::new("tesseract");
    server::new(move|| create_app(db.clone(), db_type.clone(), schema_arc.clone(), env_vars.clone(), cache.clone()))
        .bind(&server_addr)
        .expect(&format!("cannot bind to {}", server_addr))
        .start();

    println!("Tesseract listening on: {}", server_addr);
    println!("Tesseract database:     {}, {}", db_url, db_type_viz);
    println!("Tesseract schema path:  {}", schema_path);

    sys.run();

    Ok(())
}

/// CLI arguments helper.
#[derive(Debug, StructOpt)]
#[structopt(name="tesseract")]
struct Opt {
    #[structopt(short="a", long="addr")]
    address: Option<String>,

    #[structopt(long="db-url")]
    database_url: Option<String>,
}

