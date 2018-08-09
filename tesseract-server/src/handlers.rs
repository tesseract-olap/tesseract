use serde_qs as qs;
use std::fs;
use warp;

use ::query::{AggregateQuery, FlushQuery};
use ::Tesseract;

// GET flush?=:flush_query
pub fn flush(
    flush_query: FlushQuery,
    tesseract: Tesseract,
    flush_secret: Option<String>,
    schema_filepath: String,
    ) -> Result<impl warp::Reply, warp::Rejection>
{
    // the router will already reject if no querystring with secret

    if let Some(secret) = flush_secret {
        if secret == flush_query.secret {
            info!("flush with secret match");

            // Doesn't really matter if file handling is sync here.
            // The whole server must be blocked while the schema
            // is flushed
            let schema_raw = match fs::read_to_string(schema_filepath) {
                Ok(s) => s,
                Err(err) => {
                    error!("Error reading schema file: {}", err);
                    return Err(warp::reject::server_error());
                },
            };

            let mut engine = tesseract.write().unwrap();
            match (*engine).flush(&schema_raw) {
                Ok(()) => (),
                Err(err) => {
                    error!("Error flushing tesseract: {}", err);
                    return Err(warp::reject::server_error());
                },
            }

            Ok(warp::reply::json(&json!({"flush": true})))
        } else {
            // if there's a secret set, but query secret does
            // not match, then reject
            Err(warp::reject())
        }
    } else {
        // don't flush if there's no secret, the query secret doesn't matter
        info!("no flush with no secret set");
        Err(warp::reject())
    }
}

// GET cubes/
pub fn list_cube_metadata(tesseract: Tesseract) -> impl warp::Reply {
    info!("list all cube metadata endpoint");
    warp::reply::json(&tesseract.read().unwrap().schema)
}

// GET cubes/:id/aggregate?:query
pub fn aggregate_query(cube: String, query: String) -> Result<impl warp::Reply, warp::Rejection> {
    info!("{}", query);
    lazy_static!{
        static ref QS_NON_STRICT: qs::Config = qs::Config::new(5, false);
    }
    let agg_query = QS_NON_STRICT.deserialize_str::<AggregateQuery>(&query);
    agg_query
        .map(|query| {
            warp::reply::json(
                &json!({
                    "cube": cube,
                    "query":  query,
                })
            )
        })
        .map_err(|err| {
            error!("Could not parse aggregate query string: {}", err);
            warp::reject()
        })
}
