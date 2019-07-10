use actix_web::{
    HttpRequest,
    HttpResponse,
    web::Path,
};
use futures::future;
use lazy_static::lazy_static;
use log::*;
use serde_qs as qs;
use std::convert::TryInto;
use tesseract_core::format::FormatType;
use tesseract_core::format_stream::format_records_stream;
use tesseract_core::Query as TsQuery;
use futures::future::{Future};
use failure::Error;

use crate::app::AppState;
use super::aggregate::AggregateQueryOpt;
use super::util;

/// Handles default aggregation when a format is not specified.
/// Default format is CSV.
pub fn aggregate_default_handler(req: HttpRequest, cube: Path<String>) -> impl Future<Item=HttpResponse, Error=Error>
{
    let cube_format = (cube.into_inner(), "csv".to_owned());
    do_aggregate(req, cube_format)
}

/// Handles aggregation when a format is specified.
pub fn aggregate_handler(req: HttpRequest, cube_format: Path<(String, String)>) -> impl Future<Item=HttpResponse, Error=Error>
{
    do_aggregate(req, cube_format.into_inner())
}

/// Performs data aggregation.
pub fn do_aggregate(req: HttpRequest, cube_format: (String, String)) -> impl Future<Item=HttpResponse, Error=Error>
{
    let (cube, format) = cube_format;

    let format = format.parse::<FormatType>();
    let format = match format {
        Ok(f) => f,
        Err(err) => {
            return future::result(
                Ok(HttpResponse::NotFound().json(err.to_string()))
            );
        },
    };

    info!("cube: {}, format: {:?}", cube, format);

    let query = req.query_string();
    lazy_static!{
        static ref QS_NON_STRICT: qs::Config = qs::Config::new(5, false);
    }
    let agg_query_res = QS_NON_STRICT.deserialize_str::<AggregateQueryOpt>(&query);
    let agg_query = match agg_query_res {
        Ok(q) => q,
        Err(err) => {
            return future::result(
                Ok(HttpResponse::NotFound().json(err.to_string()))
            );
        },
    };
    info!("query opts:{:?}", agg_query);

    // Turn AggregateQueryOpt into Query
    let ts_query: Result<TsQuery, _> = agg_query.try_into();
    let ts_query = match ts_query {
        Ok(q) => q,
        Err(err) => {
            return future::result(
                Ok(HttpResponse::NotFound().json(err.to_string()))
            );
        },
    };
    let app_state = req.app_data::<AppState>().unwrap();

    let query_ir_headers = app_state
        .schema.read().unwrap()
        .sql_query(&cube, &ts_query);

    let (query_ir, headers) = match query_ir_headers {
        Ok(x) => x,
        Err(err) => {
            return future::result(
                Ok(HttpResponse::NotFound().json(err.to_string()))
            );
        },
    };

    let sql = app_state
        .backend
        .generate_sql(query_ir);

    info!("Sql query: {}", sql);
    info!("Headers: {:?}", headers);

    let df_stream = app_state
        .backend
        .exec_sql_stream(sql);

    let content_type = util::format_to_content_type(&format);


    futures::future::ok(
        HttpResponse::Ok()
        .set(content_type)
        .streaming(format_records_stream(headers, df_stream, format))
    )
    //    .and_then(move |df_stream_res| {
    //        match df_stream_res {
    //            Ok(df_stream) => Ok(HttpResponse::Ok().streaming(format_records_stream(headers, df_stream, format))),
    //            Err(err) => Ok(HttpResponse::NotFound().json(err.to_string())),
    //    })
    //    .map_err(move |e| {
    //        if app_state.debug {
    //            ServerError::Db { cause: e.to_string() }.into()
    //        } else {
    //            ServerError::Db { cause: "Internal Server Error 1010".to_owned() }.into()
    //        }
    //    })
    //    .responder()
}
