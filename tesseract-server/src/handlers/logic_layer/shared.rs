use actix_web::{
    AsyncResponder,
    FutureResponse,
    HttpRequest,
    HttpResponse,
};
use failure::{Error, format_err};
use futures::future::{self, Future};
use log::*;
use serde_derive::{Serialize, Deserialize};
use std::convert::{TryFrom, TryInto};
use std::collections::HashMap;

use tesseract_core::format::{format_records, FormatType};
use tesseract_core::Query as TsQuery;

use crate::app::AppState;


#[derive(Debug, Clone)]
pub enum TimeValue {
    First,
    Last,
    Value(u32),
}

impl TimeValue {
    pub fn from_str(raw: String) -> Result<Self, Error> {
        if raw == "latest" {
            Ok(TimeValue::Last)
        } else if raw == "oldest" {
            Ok(TimeValue::First)
        } else {
            match raw.parse::<u32>() {
                Ok(n) => Ok(TimeValue::Value(n)),
                Err(_) => Err(format_err!("Wrong type for time argument."))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum TimePrecision {
    Year,
    Quarter,
    Month,
    Week,
    Day,
}

impl TimePrecision {
    pub fn from_str(raw: String) -> Result<Self, Error> {
        match raw.as_ref() {
            "year" => Ok(TimePrecision::Year),
            "quarter" => Ok(TimePrecision::Quarter),
            "month" => Ok(TimePrecision::Month),
            "week" => Ok(TimePrecision::Week),
            "day" => Ok(TimePrecision::Day),
            _ => Err(format_err!("Wrong type for time precision argument."))
        }
    }
}

#[derive(Debug, Clone)]
pub struct Time {
    pub precision: TimePrecision,
    pub value: TimeValue,
}

impl Time {
    pub fn from_str(raw: String) -> Result<Self, Error> {
        let e: Vec<&str> = raw.split(".").collect();

        if e.len() != 2 {
            return Err(format_err!("Wrong format for time argument."));
        }

        let precision = match TimePrecision::from_str(e[0].to_string()) {
            Ok(precision) => precision,
            Err(err) => return Err(err),
        };
        let value = match TimeValue::from_str(e[1].to_string()) {
            Ok(value) => value,
            Err(err) => return Err(err),
        };

        Ok(Time {precision, value})
    }

    pub fn from_key_value(key: String, value: String) -> Result<Self, Error> {
        let precision = match TimePrecision::from_str( key) {
            Ok(precision) => precision,
            Err(err) => return Err(err),
        };
        let value = match TimeValue::from_str(value) {
            Ok(value) => value,
            Err(err) => return Err(err),
        };

        Ok(Time {precision, value})
    }
}


/// The last few aggregation operations are common across all different routes.
/// This method implements that step to avoid duplication.
pub fn finish_aggregation(
    req: HttpRequest<AppState>,
    mut agg_query: LogicLayerQueryOpt,
    format: FormatType
) -> FutureResponse<HttpResponse> {
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
                        let cut = match info.get_time_cut(time) {
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LogicLayerQueryOpt {
    cube: String,
    drilldowns: Option<Vec<String>>,
    cuts: Option<Vec<String>>,
    measures: Option<Vec<String>>,
    time: Option<HashMap<String, String>>,
    properties: Option<Vec<String>>,
    parents: Option<bool>,
    top: Option<String>,
    top_where: Option<String>,
    sort: Option<String>,
    limit: Option<String>,
    growth: Option<String>,
    rca: Option<String>,
    debug: Option<bool>,
//    distinct: Option<bool>,
//    nonempty: Option<bool>,
//    sparse: Option<bool>,
}

impl LogicLayerQueryOpt {
    fn deserialize_args(arg: String) -> Vec<String> {
        // TODO: Not working when only one of the arguments is wrapped in []

        let arg_vec: Vec<String> = (if arg.chars().nth(0).unwrap() == '[' {
            // check if starts with '[', then assume
            // that this means that it's a qualified name
            // with [] wrappers. This means that can't just
            // split on any periods, only periods that fall
            // outside the []
            let pattern: &[_] = &['[', ']'];
            let arg = arg.trim_matches(pattern);
            arg.split("],[")
        } else {
            arg.split(",")
        }).map(|s| s.to_string()).collect();

        arg_vec
    }

    pub fn from_params_list(params_list: Vec<(String, String)>) -> Result<Self, Error> {
        let mut cube: String = "".to_string();
        let mut drilldowns: Option<Vec<String>> = None;
        let mut cuts: Option<Vec<String>> = None;
        let mut measures: Option<Vec<String>> = None;
        let mut time: Option<HashMap<String, String>> = None;
        let mut properties: Option<Vec<String>> = None;
        let mut parents: Option<bool> = None;
        let mut top: Option<String> = None;
        let mut top_where: Option<String> = None;
        let mut sort: Option<String> = None;
        let mut limit: Option<String> = None;
        let mut growth: Option<String> = None;
        let mut rca: Option<String> = None;
        let mut debug: Option<bool> = None;

        let mut time_map: HashMap<String, String> = HashMap::new();

        for p in params_list {
            let param = p.0;
            let value = p.1;

            if param == "cube" {
                cube = value;
            } else if param == "drilldowns" {
                drilldowns = Some(LogicLayerQueryOpt::deserialize_args(value));
            } else if param == "cuts" {
                cuts = Some(LogicLayerQueryOpt::deserialize_args(value));
            } else if param == "measures" {
                measures = Some(LogicLayerQueryOpt::deserialize_args(value));
            } else if param == "time" {
                let time_op: Vec<String> = value.split(".").map(|s| s.to_string()).collect();
                if time_op.len() != 2 {
                    return Err(format_err!("Wrong format for time argument."));
                }
                time_map.insert(time_op[0].clone(), time_op[1].clone());
            } else if param == "properties" {
                properties = Some(LogicLayerQueryOpt::deserialize_args(value));
            } else if param == "parents" {
                if value == "true" {
                    parents = Some(true);
                } else {
                    parents = Some(false);
                }
            } else if param == "top" {
                top = Some(value);
            } else if param == "top_where" {
                top_where = Some(value);
            } else if param == "sort" {
                sort = Some(value);
            } else if param == "limit" {
                limit = Some(value);
            } else if param == "growth" {
                growth = Some(value);
            } else if param == "rca" {
                rca = Some(value);
            } else if param == "debug" {
                if value == "true" {
                    debug = Some(true);
                } else {
                    debug = Some(false);
                }
            }
        }

        if time_map.len() >= 1 {
            time = Some(time_map);
        }

        Ok(
            LogicLayerQueryOpt {
                cube,
                drilldowns,
                cuts,
                measures,
                time,
                properties,
                parents,
                top,
                top_where,
                sort,
                limit,
                growth,
                rca,
                debug
            }
        )
    }
}

impl TryFrom<LogicLayerQueryOpt> for TsQuery {
    type Error = Error;

    fn try_from(agg_query_opt: LogicLayerQueryOpt) -> Result<Self, Self::Error> {
        let drilldowns: Vec<_> = agg_query_opt.drilldowns
            .map(|ds| {
                ds.iter().map(|d| d.parse()).collect()
            })
            .unwrap_or(Ok(vec![]))?;

        let cuts: Vec<_> = agg_query_opt.cuts
            .map(|cs| {
                cs.iter().map(|c| c.parse()).collect()
            })
            .unwrap_or(Ok(vec![]))?;

        let measures: Vec<_> = agg_query_opt.measures
            .map(|ms| {
                ms.iter().map(|m| m.parse()).collect()
            })
            .unwrap_or(Ok(vec![]))?;

        let properties: Vec<_> = agg_query_opt.properties
            .map(|ms| {
                ms.iter().map(|m| m.parse()).collect()
            })
            .unwrap_or(Ok(vec![]))?;

        let parents = agg_query_opt.parents.unwrap_or(false);

        let top = agg_query_opt.top
            .map(|t| t.parse())
            .transpose()?;
        let top_where = agg_query_opt.top_where
            .map(|t| t.parse())
            .transpose()?;
        let sort = agg_query_opt.sort
            .map(|s| s.parse())
            .transpose()?;
        let limit = agg_query_opt.limit
            .map(|l| l.parse())
            .transpose()?;

        let growth = agg_query_opt.growth
            .map(|g| g.parse())
            .transpose()?;

        let rca = agg_query_opt.rca
            .map(|r| r.parse())
            .transpose()?;

        let debug = agg_query_opt.debug.unwrap_or(false);

        Ok(TsQuery {
            drilldowns,
            cuts,
            measures,
            parents,
            properties,
            top,
            top_where,
            sort,
            limit,
            rca,
            growth,
            debug,
        })
    }
}
