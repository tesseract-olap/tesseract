use actix_web::{
    FutureResponse,
    HttpRequest,
    HttpResponse,
    Path,
};
use futures::future;
use lazy_static::lazy_static;
use log::*;
use serde_qs as qs;
use std::convert::TryInto;
use tesseract_core::format::FormatType;
use tesseract_core::format_stream::format_records_stream;
use tesseract_core::Query as TsQuery;

use crate::app::AppState;
use super::aggregate::AggregateQueryOpt;
use super::util::{boxed_error_http_response, verify_api_key, format_to_content_type};


/// Handles default aggregation when a format is not specified.
/// Default format is CSV.
pub fn aggregate_default_handler(
    (req, cube): (HttpRequest<AppState>, Path<String>)
    ) -> FutureResponse<HttpResponse>
{
    let cube_format = (cube.into_inner(), "csv".to_owned());
    do_aggregate(req, cube_format)
}


/// Handles aggregation when a format is specified.
pub fn aggregate_handler(
    (req, cube_format): (HttpRequest<AppState>, Path<(String, String)>)
    ) -> FutureResponse<HttpResponse>
{
    do_aggregate(req, cube_format.into_inner())
}


/// Performs data aggregation.
pub fn do_aggregate(
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

    info!("cube: {}, format: {:?}", cube, format);

    let query = req.query_string();
    lazy_static!{
        static ref QS_NON_STRICT: qs::Config = qs::Config::new(5, false);
    }
    let agg_query_res = QS_NON_STRICT.deserialize_str::<AggregateQueryOpt>(&query);
    let agg_query = ok_or_404!(agg_query_res);

    info!("query opts:{:?}", agg_query);

    // Turn AggregateQueryOpt into Query
    let ts_query: Result<TsQuery, _> = agg_query.try_into();
    let ts_query = ok_or_404!(ts_query);

    let query_ir_headers = req
        .state()
        .schema.read().unwrap()
        .sql_query(&cube, &ts_query, None);

    let (query_ir, headers) = ok_or_404!(query_ir_headers);

    let sql = req.state()
        .backend
        .generate_sql(query_ir);

    info!("Sql query: {}", sql);
    info!("Headers: {:?}", headers);

    let df_stream = req.state()
        .backend
        .exec_sql_stream(sql);

    let content_type = format_to_content_type(&format);

    Box::new(
        futures::future::ok(
            HttpResponse::Ok()
            .set(content_type)
            .streaming(format_records_stream(headers, df_stream, format, false))
        )
    )
    //    .and_then(move |df_stream_res| {
    //        match df_stream_res {
    //            Ok(df_stream) => Ok(HttpResponse::Ok().streaming(format_records_stream(headers, df_stream, format))),
    //            Err(err) => Ok(HttpResponse::NotFound().json(err.to_string())),
    //    })
    //    .map_err(move |e| {
    //        if req.state().debug {
    //            ServerError::Db { cause: e.to_string() }.into()
    //        } else {
    //            ServerError::Db { cause: "Internal Server Error 1010".to_owned() }.into()
    //        }
    //    })
    //    .responder()
}

