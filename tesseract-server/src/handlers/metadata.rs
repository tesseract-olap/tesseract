use actix_web::{
    AsyncResponder,
    FutureResponse,
    HttpRequest,
    HttpResponse,
    Path,
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
use super::util::{boxed_error_http_response, verify_api_key};


pub fn metadata_handler(
    (req, cube): (HttpRequest<AppState>, Path<String>)
    ) -> ActixResult<HttpResponse>
{
    info!("Metadata for cube: {}", cube);

    // currently, we do not check that cube names are distinct
    // TODO fix this
    match req.state().schema.read().unwrap().cube_metadata(&cube) {
        Some(cube) => Ok(HttpResponse::Ok().json(cube)),
        None => Ok(HttpResponse::NotFound().finish()),
    }
}


pub fn metadata_all_handler(
    req: HttpRequest<AppState>
    ) -> ActixResult<HttpResponse>
{
    info!("Metadata for all");

    Ok(HttpResponse::Ok().json(req.state().schema.read().unwrap().metadata()))
}


pub fn members_default_handler(
    (req, cube): (HttpRequest<AppState>, Path<String>)
    ) -> FutureResponse<HttpResponse>
{
    let cube_format = (cube.into_inner(), "csv".to_owned());
    do_members(req, cube_format)
}


pub fn members_handler(
    (req, cube_format): (HttpRequest<AppState>, Path<(String, String)>)
    ) -> FutureResponse<HttpResponse>
{
    do_members(req, cube_format.into_inner())
}


pub fn do_members(
    req: HttpRequest<AppState>,
    cube_format: (String, String),
    ) -> FutureResponse<HttpResponse>
{
    let (cube, format) = cube_format;

    // Get cube object to check for API key
    let schema = &req.state().schema.read().unwrap().clone();
    let cube_obj = ok_or_404!(schema.get_cube_by_name(&cube));

    match verify_api_key(&req, &cube_obj) {
        Ok(_) => (),
        Err(err) => return boxed_error_http_response(err)
    }

    let format = ok_or_404!(format.parse::<FormatType>());

    let query = req.query_string();

    lazy_static!{
        static ref QS_NON_STRICT: qs::Config = qs::Config::new(5, false);
    }

    let query_res = QS_NON_STRICT.deserialize_str::<MembersQueryOpt>(&query);
    let query = ok_or_400!(query_res);

    let level: LevelName = ok_or_400!(query.level.parse());

    info!("Members for cube: {}, level: {}", cube, level);

    let members_sql_and_headers = req.state().schema.read().unwrap()
        .members_sql(&cube, &level);
    let (members_sql, header) = ok_or_400!(members_sql_and_headers);

    req.state()
        .backend
        .exec_sql(members_sql)
        .from_err()
        .and_then(move |df| {
            match format_records(&header, df, format) {
                Ok(res) => Ok(HttpResponse::Ok().body(res)),
                Err(err) => Ok(HttpResponse::NotFound().json(err.to_string())),
            }
        })
        .responder()
}

#[derive(Debug, Deserialize)]
struct MembersQueryOpt {
    level: String,
}
