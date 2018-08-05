use warp;
use warp::http::StatusCode;

use ::env_vars::EnvVars;
use ::query::FlushQuery;
use ::schema::Schema;

// GET flush?=:flush_query
pub fn flush(_flush_query: FlushQuery, _schema: Schema, _env_vars: EnvVars) -> Result<impl warp::Reply, warp::Rejection> {
    info!("flush endpoint");
    Ok(StatusCode::OK)
}

// GET cubes/
pub fn list_cube_metadata(schema: Schema) -> impl warp::Reply {
    info!("list all cube metadata endpoint");
    warp::reply::json(&*schema.lock().unwrap())
}
