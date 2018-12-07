use actix_web::{
    HttpRequest,
    HttpResponse,
    Path,
    Result as ActixResult
};
use log::*;
use tesseract_core::Backend;

use crate::app::AppState;

pub fn metadata_handler<B: Backend>(
    (req, cube): (HttpRequest<AppState<B>>, Path<String>)
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

pub fn metadata_all_handler<B: Backend>(
    req: HttpRequest<AppState<B>>
    ) -> ActixResult<HttpResponse>
{
    info!("Metadata for all");

    Ok(HttpResponse::Ok().json(req.state().schema.clone()))
}

