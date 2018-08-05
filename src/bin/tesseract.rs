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
            format!("The cube you're getting info about: {}", cube)
        });

    let dimensions = cubes_id
        .and(warp::path("dimensions"));

    let dimensions_metadata = dimensions
        .and(warp::path::index())
        .map(|cube: String| format!("list all dims for {}", cube));

    let dimensions_id = dimensions
        .and(warp::path::param::<String>());

    let dimensions_id_metadata = dimensions_id
        .and(warp::path::index())
        .map(|cube: String, dim: String| {
            format!("The cube and dim you're getting info about: {}, {}", cube, dim)
        });

    let hierarchies = dimensions_id
        .and(warp::path("hierarchies"));

    let hierarchies_metadata = hierarchies
        .and(warp::path::index())
        .map(|cube: String, dim: String| {
            format!("list all hierarchies for {}, {}", cube, dim)
        });

    let hierarchies_id = hierarchies
        .and(warp::path::param::<String>());

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

    let levels_metadata = levels
        .and(warp::path::index())
        .map(|cube: String, dim: String, hier: String| {
            format!("list all levels for {}, {}, {}", cube, dim, hier)
        });

    let levels_id = levels
        .and(warp::path::param::<String>());

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

    let members_metadata = levels_id
        .and(warp::path("members"))
        .and(warp::path::index())
        .map(|cube: String, dim: String, hier: String, level: String| {
            format!("list all members for {}, {}, {}, {}", cube, dim, hier, level)
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
    );

    warp::serve(routes)
        .run(([127,0,0,1], 7777));
}

