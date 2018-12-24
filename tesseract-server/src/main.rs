#![feature(try_from)]
#![feature(transpose_result)]

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
mod db_config;
mod handlers;

use actix_web::server;
use dotenv::dotenv;
use failure::{Error, format_err};
use std::env;
use structopt::StructOpt;

use tesseract_core::Schema;

fn main() -> Result<(), Error> {
    // Configuration

    pretty_env_logger::init();
    dotenv().ok();
    let opt = Opt::from_args();

    let server_addr = opt.address.unwrap_or("127.0.0.1:7777".to_owned());
    let schema_path = env::var("TESSERACT_SCHEMA_FILEPATH")
        .expect("TESSERACT_SCHEMA_FILEPATH not found");

    let db_url_full = env::var("TESSERACT_DATABASE_URL")
        .or(opt.database_url.ok_or(format_err!("")))
        .map_err(|_| format_err!("database url not found; either TESSERACT_DATABASE_URL or cli option required"))?;

    let (db, db_url, db_type) = db_config::get_db(&db_url_full)?;

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
    println!("Tesseract database:     {}, {}", db_url, db_type);
    println!("Tesseract schema path:  {}", schema_path);

    sys.run();

    Ok(())
}

#[derive(Debug, StructOpt)]
#[structopt(name="tesseract")]
struct Opt {
    #[structopt(short="a", long="addr")]
    address: Option<String>,

    #[structopt(long="db-url")]
    database_url: Option<String>,
}

#[derive(Debug, Clone)]
struct EnvVars {
    pub flush_secret: Option<String>,
    pub database_url: String,
    pub schema_filepath: Option<String>,
}

