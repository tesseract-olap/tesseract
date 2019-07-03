use actix_web::{
    Error,
    HttpRequest,
    HttpResponse,
    web::Path,
    Result as ActixResult
};

use futures::future::{self, Future};
use lazy_static::lazy_static;
use log::*;
use serde_derive::Deserialize;
use serde_qs as qs;
use tesseract_core::format::{format_records, FormatType};
use tesseract_core::names::LevelName;

use crate::app::AppState;

pub fn metadata_handler(req: HttpRequest, cube: Path<String>) -> HttpResponse
{
    info!("Metadata for cube: {}", cube);
    let app_state = req.app_data::<AppState>().unwrap();

    // currently, we do not check that cube names are distinct
    // TODO fix this
    match app_state.schema.read().unwrap().cube_metadata(&cube) {
        Some(cube) => HttpResponse::Ok().json(cube),
        None => HttpResponse::NotFound().finish(),
    }
}

pub fn metadata_all_handler(req: HttpRequest) -> HttpResponse
{
    info!("Metadata for all cubes");
    let app_state = req.app_data::<AppState>().unwrap();

    HttpResponse::Ok().json(app_state.schema.read().unwrap().metadata())
}


#[derive(Debug, Deserialize)]
struct MembersQueryOpt {
    level: String,
}
