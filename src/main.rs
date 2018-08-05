#[macro_use]
extern crate log;
extern crate pretty_env_logger;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate warp;

use std::env;
use warp::Filter;

mod env_vars;
mod handlers;
mod query;
mod schema;

use query::{AggregateQuery, FlushQuery};


// contains routes
fn main() {
    pretty_env_logger::init();

    // Turn schema into Filter
    let schema_data = schema::SchemaData::new();
    let schema = schema::init(schema_data);
    let schema = warp::any().map(move || schema.clone());

    let mut env_vars = env_vars::EnvVars::new();
    env_vars.secret = env::var("TESSERACT_SECRET").ok();
    let env_vars = warp::any().map(move || env_vars.clone());

    // >> Flush

    let flush = warp::path("flush")
        .and(warp::query::<FlushQuery>())
        .and(warp::path::index())
        .and(schema.clone())
        .and(env_vars)
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
        .map(handlers::list_cube_metadata);

    let cubes_id = cubes
        .and(warp::path::param::<String>());

    // GET cubes/:id
    let cubes_id_metadata = cubes_id
        .and(warp::path::index())
        .map(|cube: String| {
            format!("The cube you're getting info about: {}", cube)
        });

    let dimensions = cubes_id
        .and(warp::path("dimensions"));

    // GET cubes/:id/dimensions/
    let dimensions_metadata = dimensions
        .and(warp::path::index())
        .map(|cube: String| format!("list all dims for {}", cube));

    let dimensions_id = dimensions
        .and(warp::path::param::<String>());

    // GET cubes/:id/dimensions/:id
    let dimensions_id_metadata = dimensions_id
        .and(warp::path::index())
        .map(|cube: String, dim: String| {
            format!("The cube and dim you're getting info about: {}, {}", cube, dim)
        });

    let hierarchies = dimensions_id
        .and(warp::path("hierarchies"));

    // GET cubes/:id/dimensions/:id/hierarchies
    let hierarchies_metadata = hierarchies
        .and(warp::path::index())
        .map(|cube: String, dim: String| {
            format!("list all hierarchies for {}, {}", cube, dim)
        });

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
        });

    let levels = hierarchies_id
        .and(warp::path("levels"));

    // GET cubes/:id/dimensions/:id/hierarchies/:id/levels
    let levels_metadata = levels
        .and(warp::path::index())
        .map(|cube: String, dim: String, hier: String| {
            format!("list all levels for {}, {}, {}", cube, dim, hier)
        });

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
        });

    // GET cubes/:id/dimensions/:id/hierarchies/:id/levels/:id/members
    let members_metadata = levels_id
        .and(warp::path("members"))
        .and(warp::path::index())
        .map(|cube: String, dim: String, hier: String, level: String| {
            format!("list all members for {}, {}, {}, {}", cube, dim, hier, level)
        });

    // << end cubes basic route

    // >> aggregate route

    // default json
    // GET cubes/:id/aggregate?=:query
    let aggregate_default_query = cubes_id
        .and(warp::path("aggregate"))
        .and(warp::query::<AggregateQuery>())
        .and(warp::path::index())
        .map(|cube: String, query: AggregateQuery| {
            format!("aggregate cube default: {:?}, query: {:?}", cube, query)
        });

    // csv
    // GET cubes/:id/aggregate.csv?=:query
    let aggregate_csv_query = cubes_id
        .and(warp::path("aggregate.csv"))
        .and(warp::query::<AggregateQuery>())
        .and(warp::path::index())
        .map(|cube: String, query: AggregateQuery| {
            format!("aggregate cube csv: {:?}, query: {:?}", cube, query)
        });

    // jsonrecords
    // GET cubes/:id/aggregate.jsonrecords?=:query
    let aggregate_jsonrecords_query = cubes_id
        .and(warp::path("aggregate.jsonrecords"))
        .and(warp::query::<AggregateQuery>())
        .and(warp::path::index())
        .map(|cube: String, query: AggregateQuery| {
            format!("aggregate cube jsonrecords: {:?}, query: {:?}", cube, query)
        });

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
    );

    warp::serve(routes)
        .run(([127,0,0,1], 7777));
}

