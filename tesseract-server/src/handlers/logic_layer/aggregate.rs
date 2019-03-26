use std::collections::HashMap;
use std::convert::{TryInto};

use actix_web::{
    AsyncResponder,
    FutureResponse,
    HttpRequest,
    HttpResponse,
    Path,
};
use actix_web::http::header::ContentType;
use futures::future::{Future};
use lazy_static::lazy_static;
use log::*;
use serde_qs as qs;

use tesseract_core::format::{format_records, FormatType};
use tesseract_core::Query as TsQuery;

use crate::app::AppState;
use crate::handlers::logic_layer::shared::{LogicLayerQueryOpt, Time, boxed_error};
use crate::logic_layer::LogicLayerConfig;


/// Handles default aggregation when a format is not specified.
/// Default format is CSV.
pub fn logic_layer_default_handler(
    (req, _cube): (HttpRequest<AppState>, Path<()>)
) -> FutureResponse<HttpResponse>
{
    logic_layer_aggregation(req, "jsonrecords".to_owned())
}

/// Handles aggregation when a format is specified.
pub fn logic_layer_handler(
    (req, cube_format): (HttpRequest<AppState>, Path<(String)>)
) -> FutureResponse<HttpResponse>
{
    logic_layer_aggregation(req, cube_format.to_owned())
}


/// Performs data aggregation.
pub fn logic_layer_aggregation(
    req: HttpRequest<AppState>,
    format: String,
) -> FutureResponse<HttpResponse>
{
    let format_str = format.clone();

    let format = format.parse::<FormatType>();
    let format = match format {
        Ok(f) => f,
        Err(err) => return boxed_error(err.to_string()),
    };

    info!("format: {:?}", format);

    let query = req.query_string();
    let schema = req.state().schema.read().unwrap();

    let logic_layer_config: Option<LogicLayerConfig> = match &req.state().logic_layer_config {
        Some(llc) => Some(llc.read().unwrap().clone()),
        None => None
    };

    lazy_static!{
        static ref QS_NON_STRICT: qs::Config = qs::Config::new(5, false);
    }

    let mut cube_name;

    let mut agg_query = match QS_NON_STRICT.deserialize_str::<LogicLayerQueryOpt>(query) {
        Ok(mut q) => {
            cube_name = match logic_layer_config.clone() {
                Some(llc) => {
                    match llc.sub_cube_name(q.cube.clone()) {
                        Ok(cn) => cn,
                        Err(_) => q.cube.clone()
                    }
                },
                None => q.cube.clone()
            };

            let cube = match schema.get_cube_by_name(&cube_name) {
                Ok(c) => c.clone(),
                Err(err) => return boxed_error(err.to_string())
            };

            // Hack for now since can't provide extra arguments on try_into
            q.cube_obj = Some(cube.clone());
            q.config = logic_layer_config;
            q
        },
        Err(err) => return boxed_error(err.to_string())
    };

    // Process `time` param (latest/oldest)
    match &agg_query.time {
        Some(s) => {
            let cube_cache = req.state().cache.read().unwrap().find_cube_info(&cube_name);

            let time_cuts: Vec<String> = s.split(",").map(|s| s.to_string()).collect();

            for time_cut in time_cuts {
                let tc: Vec<String> = time_cut.split(".").map(|s| s.to_string()).collect();

                if tc.len() != 2 {
                    return boxed_error("Malformatted time cut".to_string());
                }

                let time = match Time::from_key_value(tc[0].clone(), tc[1].clone()) {
                    Ok(time) => time,
                    Err(err) => return boxed_error(err.to_string())
                };

                match cube_cache.clone() {
                    Some(cache) => {
                        let (cut, cut_value) = match cache.get_time_cut(time) {
                            Ok(cut) => cut,
                            Err(err) => return boxed_error(err.to_string())
                        };

                        agg_query.cuts = match agg_query.cuts {
                            Some(mut cuts) => {
                                cuts.insert(cut, cut_value);
                                Some(cuts)
                            },
                            None => {
                                let mut m: HashMap<String, String> = HashMap::new();
                                m.insert(cut, cut_value);
                                Some(m)
                            },
                        }
                    },
                    None => (),
                };
            }
        },
        None => (),
    }

    info!("aggregate query: {:?}", agg_query);

    // Turn AggregateQueryOpt into TsQuery
    let ts_query: Result<TsQuery, _> = agg_query.try_into();
    let ts_query = match ts_query {
        Ok(q) => q,
        Err(err) => return boxed_error(err.to_string())
    };

    info!("tesseract query: {:?}", ts_query);

    let query_ir_headers = req
        .state()
        .schema.read().unwrap()
        .sql_query(&cube_name, &ts_query);

    let (query_ir, headers) = match query_ir_headers {
        Ok(x) => x,
        Err(err) => return boxed_error(err.to_string())
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
                Ok(res) => {
                    if format_str == "jsonrecords" {
                        Ok(HttpResponse::Ok()
                            .set(ContentType::json())
                            .body(res))
                    } else {
                        Ok(HttpResponse::Ok().body(res))
                    }
                },
                Err(err) => Ok(HttpResponse::NotFound().json(err.to_string())),
            }
        })
        .responder()
}
