extern crate csv;
extern crate envy;
#[macro_use]
extern crate failure;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate pretty_env_logger;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;
extern crate serde_qs;
extern crate warp;

use failure::Error;
use std::fs;
use warp::Filter;

mod engine;
mod env_vars;
mod handlers;
mod query;
mod schema;
mod schema_config;

use env_vars::EnvVars;
use query::FlushQuery;


// contains routes
fn main() -> Result<(), Error> {
    pretty_env_logger::init();

    let env = envy::prefixed("TESSERACT_").from_env::<EnvVars>()?;
    info!("Environment variables: {:?}", env);

    // storage engine init
    // this is temporary for testing
    // create interface for engine?
    let mut db = engine::MemoryEngine::new();
    db.add_table("operating_budget".to_owned(), "test-cube/operating_budget.csv")?;
    //println!("{:#?}", db);

    // Turn schema into Filter
    let schema_filepath = env.schema_filepath.clone()
        .unwrap_or("schema.json".to_owned());
    info!("Reading schema from: {}", schema_filepath);
    let schema_raw = fs::read_to_string(&schema_filepath)?;
    let schema_config = schema_config::SchemaConfig::from_json(&schema_raw)?;
    let schema_data = schema_config.into();
    let schema = schema::init(schema_data);
    let schema = warp::any().map(move || schema.clone());

    // filters for passing on config info to routes
    let flush_secret = warp::any().map(move || env.flush_secret.clone());
    let schema_filepath = warp::any().map(move || schema_filepath.clone());

    // >> Flush

    let flush = warp::path("flush")
        .and(warp::query::<FlushQuery>())
        .and(warp::path::index())
        .and(schema.clone())
        .and(flush_secret)
        .and(schema_filepath)
        .and_then(handlers::flush);

    // << end Flush

    // >> Cubes basic route and metadata,
    // from
    // - cubes
    // - dimensions
    // - hierarchies
    // - levels
    // - members
    //
    // Each gives back metadata of itself and any child members

    let cubes = warp::path("cubes");

    // GET cubes/
    let cubes_metadata = cubes
        .and(warp::path::index())
        .and(schema.clone())
        .map(handlers::list_cube_metadata)
        .boxed();

    let cubes_id = cubes
        .and(warp::path::param::<String>());

    // GET cubes/:id
    let cubes_id_metadata = cubes_id
        .and(warp::path::index())
        .map(|cube: String| {
            format!("The cube you're getting info about: {}", cube)
        })
        .boxed();

    let dimensions = cubes_id
        .and(warp::path("dimensions"));

    // GET cubes/:id/dimensions/
    let dimensions_metadata = dimensions
        .and(warp::path::index())
        .map(|cube: String| format!("list all dims for {}", cube))
        .boxed();

    let dimensions_id = dimensions
        .and(warp::path::param::<String>());

    // GET cubes/:id/dimensions/:id
    let dimensions_id_metadata = dimensions_id
        .and(warp::path::index())
        .map(|cube: String, dim: String| {
            format!("The cube and dim you're getting info about: {}, {}", cube, dim)
        })
        .boxed();

    let hierarchies = dimensions_id
        .and(warp::path("hierarchies"));

    // GET cubes/:id/dimensions/:id/hierarchies
    let hierarchies_metadata = hierarchies
        .and(warp::path::index())
        .map(|cube: String, dim: String| {
            format!("list all hierarchies for {}, {}", cube, dim)
        })
        .boxed();

    let hierarchies_id = hierarchies
        .and(warp::path::param::<String>());

    // GET cubes/:id/dimensions/:id/hierarchies/:id
    let hierarchies_id_metadata = hierarchies_id
        .and(warp::path::index())
        .map(|cube: String, dim: String, hier: String| {
            format!("The cube, dim, hier you're getting info about: {}, {}, {}",
                cube,
                dim,
                hier,
            )
        })
        .boxed();

    let levels = hierarchies_id
        .and(warp::path("levels"));

    // GET cubes/:id/dimensions/:id/hierarchies/:id/levels
    let levels_metadata = levels
        .and(warp::path::index())
        .map(|cube: String, dim: String, hier: String| {
            format!("list all levels for {}, {}, {}", cube, dim, hier)
        })
        .boxed();

    let levels_id = levels
        .and(warp::path::param::<String>());

    // GET cubes/:id/dimensions/:id/hierarchies/:id/levels/:id
    let levels_id_metadata = levels_id
        .and(warp::path::index())
        .map(|cube: String, dim: String, hier: String, level: String| {
            format!("The cube, dim, hier, level you're getting info about: {}, {}, {}, {}",
                cube,
                dim,
                hier,
                level,
            )
        })
        .boxed();

    // GET cubes/:id/dimensions/:id/hierarchies/:id/levels/:id/members
    let members_metadata = levels_id
        .and(warp::path("members"))
        .and(warp::path::index())
        .map(|cube: String, dim: String, hier: String, level: String| {
            format!("list all members for {}, {}, {}, {}", cube, dim, hier, level)
        })
        .boxed();

    // << end cubes basic route

    // >> aggregate route

    // default json
    // GET cubes/:id/aggregate?=:query
    let aggregate_default_query = cubes_id
        .and(warp::path("aggregate"))
        .and(warp::query::raw())
        .and(warp::path::index())
        .and_then(handlers::aggregate_query)
        .boxed();

    // csv
    // GET cubes/:id/aggregate.csv?=:query
    let aggregate_csv_query = cubes_id
        .and(warp::path("aggregate.csv"))
        .and(warp::query::raw())
        .and(warp::path::index())
        .and_then(handlers::aggregate_query)
        .boxed();

    // jsonrecords
    // GET cubes/:id/aggregate.jsonrecords?=:query
    let aggregate_jsonrecords_query = cubes_id
        .and(warp::path("aggregate.jsonrecords"))
        .and(warp::query::raw())
        .and(warp::path::index())
        .and_then(handlers::aggregate_query)
        .boxed();

    // << end agg route

    // >> run the server!
    // Routes specified from most specific to least,
    // otherwise the more general will match first.
    let routes = warp::get(
        cubes_metadata
            // aggregate with query routes
            .or(aggregate_default_query)
            .or(aggregate_csv_query)
            .or(aggregate_jsonrecords_query)

            // metadata list routes
            .or(dimensions_metadata)
            .or(hierarchies_metadata)
            .or(levels_metadata)
            .or(members_metadata)

            // metadata for id routes
            .or(cubes_id_metadata)
            .or(dimensions_id_metadata)
            .or(hierarchies_id_metadata)
            .or(levels_id_metadata)

            // flush
            .or(flush)
            .with(warp::log("warp::route"))
    );

    warp::serve(routes)
        .run(([127,0,0,1], 7777));

    Ok(())
}

