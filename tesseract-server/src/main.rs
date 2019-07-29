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
mod errors;
pub mod handlers;
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

    // debug is boolean, but env var is Result.
    // cli opt overrides env var if env_var is false
    let env_var_debug = env::var("TESSERACT_DEBUG")
        .map_err(|_| format_err!(""))
        .and_then(|d| {
             d.parse::<bool>()
            .map_err(|_| format_err!("could not parse bool from env_var TESSERACT_DEBUG"))
        });
    let debug = if !opt.debug {
        if let Ok(d) = env_var_debug {
            d
        } else {
            opt.debug // false
        }
    } else {
        opt.debug // true
    };

    // streaming http response (transfer encoding chunked)
    // cli is boolean, but env var is Result.
    // cli opt overrides env var if env_var is false
    // TODO this has the same logic as for debug. make util fn?
    let env_var_streaming_response = env::var("TESSERACT_STREAMING_RESPONSE")
        .map_err(|_| format_err!(""))
        .and_then(|d| {
             d.parse::<bool>()
            .map_err(|_| format_err!("could not parse bool from env_var TESSERACT_STREAMING_RESPONSE"))
        });
    let streaming_response = if !opt.streaming_response {
        if let Ok(d) = env_var_streaming_response {
            d
        } else {
            opt.streaming_response // false
        }
    } else {
        opt.streaming_response // true
    };

    // address
    let server_addr = opt.address.unwrap_or("127.0.0.1:7777".to_owned());

    // flush
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

    let mut schema = schema_config::read_schema(&schema_path)?;
    schema.validate()?;
    let mut has_unique_levels_properties = schema.has_unique_levels_properties();
    let schema_arc = Arc::new(RwLock::new(schema.clone()));

    // Env
    let env_vars = EnvVars {
        database_url: db_url.clone(),
        schema_source,
        flush_secret,
    };

    // Logic Layer Config
    let logic_layer_config = match env::var("TESSERACT_LOGIC_LAYER_CONFIG_FILEPATH") {
        Ok(config_path) => {
            match logic_layer::read_config(&config_path) {
                Ok(config_obj) => {
                    has_unique_levels_properties = config_obj.has_unique_levels_properties(&schema)?;
                    Some(config_obj)
                },
                Err(err) => return Err(err)
            }
        },
        Err(_) => None
    };

    // Initialize actix system
    let mut sys = actix::System::new("tesseract");

    // Populate internal cache
    let cache = logic_layer::populate_cache(
        schema.clone(), &logic_layer_config, db.clone(), &mut sys
    ).map_err(|err| format_err!("Cache population error: {}", err))?;

    let cache_arc = Arc::new(RwLock::new(cache));

    // Create lock on logic layer config
    let logic_layer_config = match logic_layer_config {
        Some(ll_config) => Some(Arc::new(RwLock::new(ll_config))),
        None => None
    };

    // Initialize Server
    server::new(
        move|| create_app(
                debug,
                db.clone(),
                db_type.clone(),
                env_vars.clone(),
                schema_arc.clone(),
                cache_arc.clone(),
                logic_layer_config.clone(),
                streaming_response,
                has_unique_levels_properties,
            )
        )
        .bind(&server_addr)
        .expect(&format!("cannot bind to {}", server_addr))
        .start();

    println!("Tesseract listening on: {}", server_addr);
    println!("Tesseract database:     {}, {}", db_url, db_type_viz);
    println!("Tesseract schema path:  {}", schema_path);
    if debug {
        println!("Tesseract debug mode: ON");
    }
    if streaming_response {
        println!("Tesseract streaming mode: ON");
    }

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

    #[structopt(long="debug")]
    debug: bool,

    #[structopt(long="streaming")]
    streaming_response: bool,
}
