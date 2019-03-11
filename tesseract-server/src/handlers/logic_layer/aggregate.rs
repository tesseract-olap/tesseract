use std::collections::HashMap;
use std::convert::{TryInto};

use actix_web::{
    AsyncResponder,
    FutureResponse,
    HttpRequest,
    HttpResponse,
    Path,
};
use failure::{Error, format_err};
use log::*;
use serde_urlencoded;
use futures::future::{self, Future};

use tesseract_core::format::{format_records, FormatType};
use tesseract_core::names::{LevelName, Cut, Drilldown, Property};
use tesseract_core::Query as TsQuery;

use crate::app::AppState;
use crate::handlers::logic_layer::shared::{LogicLayerQueryOpt, Time};


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


/// Performs data aggregation.
pub fn logic_layer_aggregation(
    req: HttpRequest<AppState>,
    format: String,
) -> FutureResponse<HttpResponse>
{
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

    info!("format: {:?}", format);

    let query = req.query_string();

    let agg_query_res = serde_urlencoded::from_str::<Vec<(String, String)>>(query);
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

    println!("{:?}", agg_query);

    let mut query_opt = match LogicLayerQueryOpt::from_params_list(agg_query) {
        Ok(q) => q,
        Err(err) => {
            return Box::new(
                future::result(
                    Ok(HttpResponse::NotFound().json(err.to_string()))
                )
            );
        },
    };

    let mut agg_query = query_opt.clone();

    let cube = agg_query.cube.clone();

    // Process `time` param (latest/oldest)
    match &agg_query.time {
        Some(s) => {
            let cube_info = req.state().cache.read().unwrap().find_cube_info(&cube);

            for (k, v) in s.iter() {
                let time = match Time::from_key_value(k.clone(), v.clone()) {
                    Ok(time) => time,
                    Err(err) => {
                        return Box::new(
                            future::result(
                                Ok(HttpResponse::NotFound().json(err.to_string()))
                            )
                        );
                    },
                };

                match cube_info.clone() {
                    Some(info) => {
                        let (level, val) = match info.get_time_cut(time) {
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
                                cuts.insert(level, val);
                                Some(cuts)
                            },
                            None => {
                                let mut m: HashMap<String, String> = HashMap::new();
                                m.insert(level, val);
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
        Err(err) => {
            return Box::new(
                future::result(
                    Ok(HttpResponse::NotFound().json(err.to_string()))
                )
            );
        },
    };

    info!("tesseract query: {:?}", ts_query);

    // The logic layer only requires the level name to be provided for a query.
    // Here, we find the dimension and hierarchy names for the given level names.
    // NOTE: Failing silently for queries with multiple drilldowns if not all of
    //       the level names are found.
    let schema = req
        .state()
        .schema.read().unwrap();

    let cube_obj_res = schema.cubes.iter()
        .find(|c| &c.name == &cube)
        .ok_or(format_err!("Could not find cube"));

    let cube_obj = match cube_obj_res {
        Ok(c) => c,
        Err(err) => {
            return Box::new(
                future::result(
                    Ok(HttpResponse::NotFound().json(err.to_string()))
                )
            );
        }
    };

    let mut drilldowns: Vec<Drilldown> = vec![];
    let mut cuts: Vec<Cut> = vec![];
    let mut properties: Vec<Property> = vec![];

    let drilldown_levels = ts_query.drilldown_levels();
    let cut_levels = ts_query.cut_levels();
    let property_names = ts_query.property_names();

    for dimension in cube_obj.dimensions.clone() {
        for hierarchy in dimension.hierarchies.clone() {
            for level in hierarchy.levels.clone() {
                let level_name = LevelName {
                    dimension: dimension.name.clone(),
                    hierarchy: hierarchy.name.clone(),
                    level: level.name.clone()
                };

                // drilldowns
                if drilldown_levels.contains(&level.name) {
                    drilldowns.push(Drilldown(level_name.clone()));
                }

                // cuts
                match cut_levels.get(&level.name) {
                    Some(members) => {
                        cuts.push(
                            Cut {
                                level_name: level_name.clone(),
                                members: members.clone()
                            }
                        );
                    },
                    None => continue,
                }

                // properties
                match level.properties {
                    Some(props) => {
                        for property in props.clone() {
                            if property_names.contains(&property.name) {
                                properties.push(
                                    Property {
                                        level_name: level_name.clone(),
                                        property: property.name.clone()
                                    }
                                )
                            }
                        }
                    },
                    None => continue
                }
            }
        }
    }

    let mut query_copy = ts_query.clone();
    query_copy.drilldowns = drilldowns;
    query_copy.cuts = cuts;
    query_copy.properties = properties;
    let ts_query = query_copy;

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
