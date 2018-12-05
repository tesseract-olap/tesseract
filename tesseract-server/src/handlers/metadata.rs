use actix_web::{
    HttpRequest,
    HttpResponse,
    Path,
    Result as ActixResult
};
use log::*;

use crate::app::AppState;

pub fn metadata_handler(
    (req, cube): (HttpRequest<AppState>, Path<String>)
    ) -> ActixResult<HttpResponse>
{
    info!("Metadata for cube: {}", cube);

    // currently, we do not check that cube names are distinct
    // TODO fix this
    match req.state().schema.cube_metadata(&cube) {
        Some(cube) => Ok(HttpResponse::Ok().json(cube)),
        None => Ok(HttpResponse::NotFound().finish()),
    }
}

pub fn metadata_all_handler(
    req: HttpRequest<AppState>
    ) -> ActixResult<HttpResponse>
{
    info!("Metadata for all");

    Ok(HttpResponse::Ok().json(req.state().schema.clone()))
}

