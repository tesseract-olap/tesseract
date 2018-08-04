#[macro_use]
extern crate log;
extern crate pretty_env_logger;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate warp;

use std::collections::HashMap;
use warp::Filter;

#[derive(Debug, Deserialize)]
struct AggregateQuery {
    drilldowns: Vec<String>,
    cuts: Vec<String>,
    measures: Vec<String>,
    properties: Vec<String>,
    parents: bool,
    debug: bool,
//    distinct: bool,
//    nonempty: bool,
//    sparse: bool,
}

fn main() {
    pretty_env_logger::init();

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

    let cubes_index = cubes
        .and(warp::path::index())
        .map(|| "All cubes info");

    let cubes_id_index = cubes
        .and(warp::path::param::<String>());

    let cubes_id = cubes_id_index
        .map(|cube: String| {
            info!("{} route hit", cube);
            format!("The cube you're getting info about: {}", cube)
        });

    // << end cubes basic route

    // >> aggregate route

    // default json
    let aggregate_default_index = cubes_id_index
        .and(warp::path("aggregate"));

    let aggregate_default = aggregate_default_index
        .map(|cube: String| {
            format!("aggregate cube but no query: {:?}", cube)
        });

    let aggregate_default_query = aggregate_default_index
        .and(warp::query::<AggregateQuery>())
        .map(|cube: String, query: AggregateQuery| {
            format!("aggregate cube default: {:?}, query: {:?}", cube, query)
        });

    // csv 
    let aggregate_csv_index = cubes_id_index
        .and(warp::path("aggregate.csv"))
        .and(warp::path::index());

    let aggregate_csv = aggregate_csv_index
        .map(|cube: String| {
            format!("you specified an aggregation for {:?} without a query", cube)
        });

    let aggregate_csv_query = aggregate_csv_index
        .and(warp::query::<HashMap<String, String>>())
        .map(|cube: String, query: HashMap<String, String>| {
            format!("aggregate cube with csv: {:?}, query: {:?}", cube, query)
        });

    // jsonrecords
    let aggregate_json_records_index = cubes_id_index
        .and(warp::path("aggregate.jsonrecords"))
        .and(warp::path::index());

    let aggregate_json_records = aggregate_json_records_index
        .map(|cube: String| {
            format!("you specified an aggregation for {:?} without a query", cube)
        });

    let aggregate_json_records_query = aggregate_json_records_index
        .and(warp::query::<HashMap<String, String>>())
        .map(|cube: String, query: HashMap<String, String>| {
            format!("aggregate cube with jsonrecords: {:?}, query: {:?}", cube, query)
        });

    // << end agg route

    // >> run the server!
    // Routes specified from most specific to least,
    // otherwise the more general will match first.
    let routes = warp::get(
        cubes_index
            // aggregate with query routes
//            .or(aggregate_json_records_query)
//            .or(aggregate_csv_query)
            .or(aggregate_default_query)
//
//            // these routes send back an error,
//            // because aggregate specified without query
//            .or(aggregate_json_records)
//            .or(aggregate_csv)
//            .or(aggregate_default)
//
//            // metadata routes
//            .or(cubes_id)
    );

    warp::serve(routes)
        .run(([127,0,0,1], 7777));
}
