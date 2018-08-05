use warp::{self};

use ::schema::Schema;

// GET cubes/
pub fn list_cube_metadata(schema: Schema) -> impl warp::Reply {
    warp::reply::json(&*schema.lock().unwrap())
}
