use warp;

use ::query::FlushQuery;
use ::schema::Schema;

// GET flush?=:flush_query
pub fn flush(flush_query: FlushQuery, _schema: Schema, flush_secret: Option<String>) -> Result<impl warp::Reply, warp::Rejection> {
    // the router will already reject if no querystring with secret

    if let Some(secret) = flush_secret {
        if secret == flush_query.secret {
            info!("flush with secret match");
            // TODO flush here
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
    warp::reply::json(&*schema.lock().unwrap())
}
