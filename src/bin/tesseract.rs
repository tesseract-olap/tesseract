#[macro_use]
extern crate log;
extern crate pretty_env_logger;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate warp;

use warp::Filter;

#[derive(Debug, Deserialize)]
struct AggregateQuery {
    drilldowns: Option<Vec<String>>,
    cuts: Option<Vec<String>>,
    measures: Option<Vec<String>>,
    properties: Option<Vec<String>>,
    parents: Option<bool>,
    debug: Option<bool>,
//    distinct: Option<bool>,
//    nonempty: Option<bool>,
//    sparse: Option<bool>,
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

    let cubes_metadata = cubes
        .and(warp::path::index())
        .map(|| "All cubes info");

    let cubes_id = cubes
        .and(warp::path::param::<String>());

    let cubes_id_metadata = cubes_id
        .and(warp::path::index())
        .map(|cube: String| {
            info!("{} route hit", cube);
            format!("The cube you're getting info about: {}", cube)
        });

    // << end cubes basic route

    // >> aggregate route

    // default json
    let aggregate_default_query = cubes_id
        .and(warp::path("aggregate"))
        .and(warp::query::<AggregateQuery>())
        .and(warp::path::index())
        .map(|cube: String, query: AggregateQuery| {
            format!("aggregate cube default: {:?}, query: {:?}", cube, query)
        });

    // csv
    let aggregate_csv_query = cubes_id
        .and(warp::path("aggregate.csv"))
        .and(warp::query::<AggregateQuery>())
        .and(warp::path::index())
        .map(|cube: String, query: AggregateQuery| {
            format!("aggregate cube csv: {:?}, query: {:?}", cube, query)
        });

    // jsonrecords
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

            // metadata routes
            .or(cubes_id_metadata)
    );

    warp::serve(routes)
        .run(([127,0,0,1], 7777));
}

