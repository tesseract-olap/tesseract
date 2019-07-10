use std::collections::HashMap;
use std::convert::{TryInto};

use actix_web::{
    HttpRequest,
    HttpResponse,
    web::Path,
};
use failure::Error;

use futures::future::{Future, Either};
use lazy_static::lazy_static;
use log::*;
use serde_qs as qs;

use tesseract_core::format::{format_records, FormatType};
use tesseract_core::Query as TsQuery;

use crate::app::AppState;
use crate::handlers::logic_layer::shared::{LogicLayerQueryOpt, Time, error_helper};
use crate::errors::ServerError;
use crate::logic_layer::LogicLayerConfig;
use super::super::util;


/// Handles default aggregation when a format is not specified.
/// Default format is CSV.
pub fn logic_layer_default_handler(req: HttpRequest, _cube: Path<()>) -> impl Future<Item=HttpResponse, Error=Error>
{
    logic_layer_aggregation(req, "jsonrecords".to_owned())
}

/// Handles aggregation when a format is specified.
pub fn logic_layer_handler(req: HttpRequest, cube_format: Path<(String)>) -> impl Future<Item=HttpResponse, Error=Error>
{
    logic_layer_aggregation(req, cube_format.to_owned())
}


/// Performs data aggregation.
pub fn logic_layer_aggregation(
    req: HttpRequest,
    format: String,
) -> impl Future<Item=HttpResponse, Error=Error>
{
    let format = format.parse::<FormatType>();
    let format = match format {
        Ok(f) => f,
        Err(err) => return Either::A(error_helper(err.to_string())),
    };

    info!("format: {:?}", format);
    let app_state = req.app_data::<AppState>().unwrap();

    let query = req.query_string();
    let schema = app_state.schema.read().unwrap();
    let debug = app_state.debug;

    let logic_layer_config: Option<LogicLayerConfig> = match &app_state.logic_layer_config {
        Some(llc) => Some(llc.read().unwrap().clone()),
        None => None
    };

    lazy_static!{
        static ref QS_NON_STRICT: qs::Config = qs::Config::new(5, false);
    }

    let mut cube_name;

    let mut agg_query = match QS_NON_STRICT.deserialize_str::<LogicLayerQueryOpt>(query) {
        Ok(mut q) => {
            // Check to see if the logic layer config has a alias with the
            // provided cube name
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
                Err(err) => return Either::A(error_helper(err.to_string()))
            };

            let cube_cache = app_state.cache.read().unwrap().find_cube_info(&cube_name).clone();

            println!(" ");
            println!("{:?}", cube_cache);
            println!(" ");

            // Hack for now since can't provide extra arguments on try_into
            q.cube_obj = Some(cube.clone());
            q.cube_cache = cube_cache;
            q.config = logic_layer_config;
            q.schema = Some(schema.clone());
            q
        },
        Err(err) => return Either::A(error_helper(err.to_string()))
    };

    // Process `time` param (latest/oldest)
    match &agg_query.time {
        Some(s) => {
            let cube_cache = app_state.cache.read().unwrap().find_cube_info(&cube_name);

            let time_cuts: Vec<String> = s.split(",").map(|s| s.to_string()).collect();

            for time_cut in time_cuts {
                let tc: Vec<String> = time_cut.split(".").map(|s| s.to_string()).collect();

                if tc.len() != 2 {
                    return Either::A(error_helper("Malformatted time cut".to_string()));
                }

                let time = match Time::from_key_value(tc[0].clone(), tc[1].clone()) {
                    Ok(time) => time,
                    Err(err) => return Either::A(error_helper(err.to_string()))
                };

                match cube_cache.clone() {
                    Some(cache) => {
                        let (cut, cut_value) = match cache.get_time_cut(time) {
                            Ok(cut) => cut,
                            Err(err) => return Either::A(error_helper(err.to_string()))
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

    info!("Aggregate query: {:?}", agg_query);

    // Turn AggregateQueryOpt into TsQuery
    let ts_query: Result<TsQuery, _> = agg_query.try_into();
    let ts_query = match ts_query {
        Ok(q) => q,
        Err(err) => return Either::A(error_helper(err.to_string()))
    };

    info!("Tesseract query: {:?}", ts_query);

    let query_ir_headers = app_state
        .schema.read().unwrap()
        .sql_query(&cube_name, &ts_query);
    let (query_ir, headers) = match query_ir_headers {
        Ok(x) => x,
        Err(err) => return Either::A(error_helper(err.to_string()))
    };

    info!("Query IR: {:?}", query_ir);

    let sql = app_state
        .backend
        .generate_sql(query_ir);

    info!("SQL query: {}", sql);
    info!("Headers: {:?}", headers);

    Either::B(app_state
        .backend
        .exec_sql(sql)
        .and_then(move |df| {
            let content_type = util::format_to_content_type(&format);

            match format_records(&headers, df, format) {
                Ok(res) => {
                    Ok(HttpResponse::Ok()
                        .set(content_type)
                        .body(res))
                },
                Err(err) => Ok(HttpResponse::NotFound().json(err.to_string())),
            }
        })
        .map_err(move |e| {
            if debug {
                ServerError::Db { cause: e.to_string() }.into()
            } else {
                ServerError::Db { cause: "Internal Server Error 1010".to_owned() }.into()
            }
        }))
}