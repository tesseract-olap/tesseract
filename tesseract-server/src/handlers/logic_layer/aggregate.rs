use actix_web::{
    AsyncResponder,
    FutureResponse,
    HttpRequest,
    HttpResponse,
    Path,
};
use failure::Error;
use futures::future::{self, Future};
use lazy_static::lazy_static;
use log::*;
use serde_derive::{Serialize, Deserialize};
use serde_qs as qs;
use std::convert::{TryFrom, TryInto};
use tesseract_core::format::{format_records, FormatType};
use tesseract_core::Query as TsQuery;

use crate::app::AppState;
use crate::handlers::aggregate::AggregateQueryOpt;


/// Handles default aggregation when a format is not specified.
/// Default format is CSV.
pub fn ll_aggregate_default_handler(
    (req, cube): (HttpRequest<AppState>, Path<String>)
) -> FutureResponse<HttpResponse>
{
    let cube_format = (cube.into_inner(), "csv".to_owned());
    ll_do_aggregate(req, cube_format)
}

/// Handles aggregation when a format is specified.
pub fn ll_aggregate_handler(
    (req, cube_format): (HttpRequest<AppState>, Path<(String, String)>)
) -> FutureResponse<HttpResponse>
{
    ll_do_aggregate(req, cube_format.into_inner())
}

/// Performs data aggregation.
pub fn ll_do_aggregate(
    req: HttpRequest<AppState>,
    cube_format: (String, String),
) -> FutureResponse<HttpResponse>
{
    let (cube, format) = cube_format;

    let format = format.parse::<FormatType>();
    let format = match format {
        Ok(f) => f,
        Err(err) => {
            return Box::new(
                future::result(
                    Ok(HttpResponse::NotFound().json(err.to_string()))
                )
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
            return Box::new(
                future::result(
                    Ok(HttpResponse::NotFound().json(err.to_string()))
                )
            );
        },
    };

    finish_aggregation(req, agg_query, cube, format)
}

/// The last few aggregation operations are common across all different routes.
/// This method implements that step to avoid duplication.
pub fn finish_aggregation(
    req: HttpRequest<AppState>,
    mut agg_query: AggregateQueryOpt,
    cube: String,
    format: FormatType
) -> FutureResponse<HttpResponse> {
    // Process `year` param (latest/oldest)
    match &agg_query.year {
        Some(s) => {
            let cube_info = req.state().cache.read().unwrap().find_cube_info(&cube);

            match cube_info {
                Some(info) => {
                    let cut = match info.get_year_cut(s.to_string()) {
                        Ok(cut) => cut,
                        Err(err) => {
                            return Box::new(
                                future::result(
                                    Ok(HttpResponse::NotFound().json(err.to_string()))
                                )
                            );
                        }
                    };

                    agg_query.cuts = match agg_query.cuts {
                        Some(mut cuts) => {
                            cuts.push(cut);
                            Some(cuts)
                        },
                        None => Some(vec![cut]),
                    }
                },
                None => (),
            };
        },
        None => (),
    }
    info!("query opts:{:?}", agg_query);

    // Turn AggregateQueryOpt into TsQuery
    let ts_query: Result<TsQuery, _> = agg_query.try_into();
    let ts_query = match ts_query {
        Ok(q) => q,
        Err(err) => {
            return Box::new(
                future::result(
                    Ok(HttpResponse::NotFound().json(err.to_string()))
                )
            );
        },
    };

    let query_ir_headers = req
        .state()
        .schema.read().unwrap()
        .sql_query(&cube, &ts_query);

    let (query_ir, headers) = match query_ir_headers {
        Ok(x) => x,
        Err(err) => {
            return Box::new(
                future::result(
                    Ok(HttpResponse::NotFound().json(err.to_string()))
                )
            );
        },
    };

    let sql = req.state()
        .backend
        .generate_sql(query_ir);

    info!("Sql query: {}", sql);
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
