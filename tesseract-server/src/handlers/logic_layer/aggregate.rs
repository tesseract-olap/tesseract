use std::collections::HashMap;
use std::convert::{TryInto};

use actix_web::{
    AsyncResponder,
    FutureResponse,
    HttpRequest,
    HttpResponse,
    Path,
};
use futures::future::{self, Future};
use lazy_static::lazy_static;
use log::*;
use serde_qs as qs;

use tesseract_core::format::{format_records, FormatType};
use tesseract_core::Query as TsQuery;

use crate::app::AppState;
use crate::handlers::logic_layer::shared::{LogicLayerQueryOpt, LogicLayerQueryOptTest, Time};


/// Handles default aggregation when a format is not specified.
/// Default format is CSV.
pub fn logic_layer_default_handler(
    (req, _cube): (HttpRequest<AppState>, Path<()>)
) -> FutureResponse<HttpResponse>
{
    logic_layer_aggregation(req, "csv".to_owned())
}

/// Handles aggregation when a format is specified.
pub fn logic_layer_handler(
    (req, cube_format): (HttpRequest<AppState>, Path<(String)>)
) -> FutureResponse<HttpResponse>
{
    logic_layer_aggregation(req, cube_format.to_owned())
}


/// Helper method to return errors (FutureResponse<HttpResponse>).
pub fn return_error(message: String) -> FutureResponse<HttpResponse> {
    Box::new(
        future::result(
            Ok(HttpResponse::NotFound().json(message))
        )
    )
}


/// Performs data aggregation.
pub fn logic_layer_aggregation(
    req: HttpRequest<AppState>,
    format: String,
) -> FutureResponse<HttpResponse>
{
    let format = format.parse::<FormatType>();
    let format = match format {
        Ok(f) => f,
        Err(err) => return return_error(err.to_string()),
    };

    info!("format: {:?}", format);

    let query = req.query_string();
    let schema = req.state().schema.read().unwrap();

    lazy_static!{
        static ref QS_NON_STRICT: qs::Config = qs::Config::new(5, false);
    }

    let agg_query = match QS_NON_STRICT.deserialize_str::<LogicLayerQueryOptTest>(query) {
        Ok(mut q) => {
            let cube = match schema.get_cube_by_name(&q.cube) {
                Ok(c) => c.clone(),
                Err(err) => return return_error(err.to_string())
            };
            // Hack for now since can't provide extra arguments on try_into
            q.cube_obj = Some(cube.clone());
            q
        },
        Err(err) => return return_error(err.to_string())
    };

//    // Process `time` param (latest/oldest)
//    match &agg_query.time {
//        Some(s) => {
//            let cube_cache = req.state().cache.read().unwrap().find_cube_info(&cube_name);
//
//            for (k, v) in s.iter() {
//                let time = match Time::from_key_value(k.clone(), v.clone()) {
//                    Ok(time) => time,
//                    Err(err) => return return_error(err.to_string())
//                };
//
//                match cube_cache.clone() {
//                    Some(cache) => {
//                        let cut = match cache.get_time_cut(time) {
//                            Ok(cut) => cut,
//                            Err(err) => return return_error(err.to_string())
//                        };
//
//                        agg_query.cuts = match agg_query.cuts {
//                            Some(mut cuts) => {
//                                cuts.push(cut);
//                                Some(cuts)
//                            },
//                            None => {
//                                Some(vec![cut])
//                            },
//                        }
//                    },
//                    None => (),
//                };
//            }
//        },
//        None => (),
//    }

    info!("aggregate query: {:?}", agg_query);

    let cube_name = agg_query.cube.clone();

    // TODO: Remove serde_urlencoded
    // Turn AggregateQueryOpt into TsQuery
    let ts_query: Result<TsQuery, _> = agg_query.try_into();
    let ts_query = match ts_query {
        Ok(q) => q,
        Err(err) => return return_error(err.to_string())
    };

    info!("tesseract query: {:?}", ts_query);

    let query_ir_headers = req
        .state()
        .schema.read().unwrap()
        .sql_query(&cube_name, &ts_query);

    let (query_ir, headers) = match query_ir_headers {
        Ok(x) => x,
        Err(err) => return return_error(err.to_string())
    };

    let sql = req.state()
        .backend
        .generate_sql(query_ir);

    info!("SQL query: {}", sql);
    info!("Headers: {:?}", headers);

    req.state()
        .backend
        .exec_sql(sql)
        .from_err()
        .and_then(move |df| {
            match format_records(&headers, df, format) {
                Ok(res) => Ok(HttpResponse::Ok().body(res)),
                Err(err) => Ok(HttpResponse::NotFound().json(err.to_string())),
            }
        })
        .responder()
}
