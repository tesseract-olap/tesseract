use actix_web::{
    // Error,
    HttpRequest,
    HttpResponse,
    web::Path,
    Result as ActixResult
};
use failure::Error;
use futures::future::Either;
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

pub fn members_default_handler(req: HttpRequest, cube: Path<String>) -> impl Future<Item=HttpResponse, Error=Error>
{
    let cube_format = (cube.into_inner(), "csv".to_owned());
    do_members(req, cube_format)
}

pub fn do_members(
    req: HttpRequest,
    cube_format: (String, String),
    ) -> impl Future<Item=HttpResponse, Error=Error>
{
    let (cube, format) = cube_format;

    let format = format.parse::<FormatType>();
    let format = match format {
        Ok(f) => f,
        Err(err) => {
            return Either::A(
                future::result(
                    Ok(HttpResponse::NotFound().json(err.to_string()))
                )
            );
        },
    };

    let query = req.query_string();
    lazy_static!{
        static ref QS_NON_STRICT: qs::Config = qs::Config::new(5, false);
    }

    let query_res = QS_NON_STRICT.deserialize_str::<MembersQueryOpt>(&query);
    let query = match query_res {
        Ok(q) => q,
        Err(err) => {
            return Either::A(
                future::result(
                    Ok(HttpResponse::BadRequest().json(err.to_string()))
                )
            );
        },
    };

    let level: LevelName = match query.level.parse() {
        Ok(q) => q,
        Err(err) => {
            return Either::A(
                future::result(
                    Ok(HttpResponse::BadRequest().json(err.to_string()))
                )
            );
        },
    };
    info!("Members for cube: {}, level: {}", cube, level);
    let app_state = req.app_data::<AppState>().unwrap();

    let members_sql_and_headers = app_state.schema.read().unwrap()
        .members_sql(&cube, &level);
        let (members_sql, header) = match members_sql_and_headers {
            Ok(s) => s,
            Err(err) => {
                return Either::A(
                    future::result(
                        Ok(HttpResponse::BadRequest().json(err.to_string()))
                    )
                );
            },
        };

    let res = app_state
        .backend
        .exec_sql(members_sql)
        .from_err()
        .and_then(move |df| {
            match format_records(&header, df, format) {
                Ok(res) => Ok(HttpResponse::Ok().body(res)),
                Err(err) => Ok(HttpResponse::NotFound().json(err.to_string())),
            }
        });

    return Either::B(res);
}

#[derive(Debug, Deserialize)]
struct MembersQueryOpt {
    level: String,
}
