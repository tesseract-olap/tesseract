/// tesseract-core contains Schema;
/// Schema is stateless; it is constructed from the schema file.
/// Schema is held in the AppState struct to provide access from a route
///
/// Each route instance will apply a tesseract_core::Query to tesseract_core::Schema to get sql.
/// The route instance then sends sql to database and gets results back in a
/// tesseract_core::Dataframe
///
/// Dataframe is then applied to Schema to format result. (for now, jsonrecords only)
///
///
/// Backend trait: exec() takes in a sql string, outputs a dataframe.

mod app;
mod clickhouse;
mod handlers;

use actix_web::server;
use clickhouse_rs::Options as ChOptions;
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
    let clickhouse_db_url = env::var("CLICKHOUSE_DATABASE_URL")
        .or(opt.clickhouse_db_url.ok_or(format_err!("")))
        .expect("No Clickhouse DB url found");
    let schema_path = env::var("TESSERACT_SCHEMA_FILEPATH")
        .clone()
        .unwrap_or("schema.json".to_owned());

    // Initialize Clickhouse
    let ch_options = ChOptions::new(
        clickhouse_db_url
            .parse()
            .expect("Could not parse CH db url")
    );

    // Initialize Schema
    let schema_str = std::fs::read_to_string(&schema_path)?;
    let schema = Schema::from_json(&schema_str)?;

    // Initialize Server
    let sys = actix::System::new("tesseract");
    server::new(move|| app::create_app(ch_options.clone(), schema.clone()))
        .bind(&server_addr)
        .expect(&format!("cannot bind to {}", server_addr))
        .start();

    println!("Tesseract listening on: {}", server_addr);
    println!("Tesseract clickhouse:   {}", clickhouse_db_url);
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
    clickhouse_db_url: Option<String>,
}

#[derive(Debug, Clone)]
struct EnvVars {
    pub flush_secret: Option<String>,
    pub database_url: String,
    pub schema_filepath: Option<String>,
}

