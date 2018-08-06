use std::fs;
use warp;

use ::query::FlushQuery;
use ::schema::{self, Schema};
use ::schema_config;

// GET flush?=:flush_query
pub fn flush(
    flush_query: FlushQuery,
    schema: Schema,
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
            let schema_config = match schema_config::SchemaConfig::from_json(&schema_raw) {
                Ok(s) => s,
                Err(err) => {
                    error!("Error parsing schema file: {}", err);
                    return Err(warp::reject::server_error());
                },
            };

            let schema_data = schema_config.into();

            schema::flush(schema, schema_data);

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
pub fn list_cube_metadata(schema: Schema) -> impl warp::Reply {
    info!("list all cube metadata endpoint");
    warp::reply::json(&*schema.read().unwrap())
}
