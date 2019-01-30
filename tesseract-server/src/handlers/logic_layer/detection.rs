use actix_web::{
    AsyncResponder,
    FutureResponse,
    HttpRequest,
    HttpResponse,
    Path,
};
use failure::{Error, format_err};
use futures::future::{self, Future};
use lazy_static::lazy_static;
use log::*;
use serde_derive::{Serialize, Deserialize};
use serde_qs as qs;
use std::convert::{TryFrom, TryInto};
use tesseract_core::{Schema, Cube};
use tesseract_core::format::{format_records, FormatType};
use tesseract_core::Query as TsQuery;

use crate::app::AppState;


/// Handles default aggregation when a format is not specified.
/// Default format is CSV.
pub fn ll_detect_default_handler(
    (req, cube): (HttpRequest<AppState>, Path<()>)
) -> FutureResponse<HttpResponse>
{
    ll_do_detection(req, "csv".to_owned())
}

/// Handles aggregation when a format is specified.
pub fn ll_detect_handler(
    (req, cube_format): (HttpRequest<AppState>, Path<(String)>)
) -> FutureResponse<HttpResponse>
{
    ll_do_detection(req, cube_format.to_owned())
}

/// Detects which cube to use based on the drilldowns, measure and cut provided.
/// In case the arguments are present in more than one cube, the first cube to match all
/// requirements is returned.
pub fn detect_cube(schema: Schema, agg_query: AggregateQueryOpt) -> Result<String, Error> {
    // TODO: Don't assume this is coming in 3's
    let drilldowns = match agg_query.drilldowns {
        Some(drilldowns) => drilldowns,
        None => vec![],
    };

    // TODO: Don't assume this is coming in 3's
    let cuts = match agg_query.cuts {
        Some(cuts) => cuts,
        None => vec![],
    };

    let measures = match agg_query.measures {
        Some(measures) => measures,
        None => vec![],
    };

    // TODO: Avoid clone here?
    for cube in schema.cubes {
        let dimension_names = get_all_dimension_names(cube.clone());
        let measure_names = get_all_measure_names(cube.clone());

        for drilldown in &drilldowns {
            if !dimension_names.contains(drilldown) {
                break;
            }
        }

        for cut in &cuts {
            if !dimension_names.contains(cut) {
                break;
            }
        }

        for measure in &measures {
            if !measure_names.contains(measure) {
                break;
            }
        }

        return Ok(String::from(cube.name));
    }

    Err(format_err!("No cubes found with the requested drilldowns/cuts/measures."))
}

pub fn get_all_dimension_names(cube: Cube) -> Vec<String> {
    let mut dimension_names: Vec<String> = vec![];

    for dimension in cube.dimensions {
        let dimension_name = dimension.name;
        for hierarchy in dimension.hierarchies {
            let hierarchy_name = hierarchy.name;
            for level in hierarchy.levels {
                let level_name = level.name;
                dimension_names.push(format!("{}.{}.{}", dimension_name, hierarchy_name, level_name).to_string());
            }
        }
    }

    dimension_names
}

pub fn get_all_measure_names(cube: Cube) -> Vec<String> {
    let mut measure_names: Vec<String> = vec![];

    for measure in cube.measures {
        measure_names.push(measure.name);
    }

    measure_names
}


/// Performs data aggregation.
pub fn ll_do_detection(
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
    lazy_static!{
        static ref QS_NON_STRICT: qs::Config = qs::Config::new(5, false);
    }
    let agg_query_res = QS_NON_STRICT.deserialize_str::<AggregateQueryOpt>(&query);
    let mut agg_query = match agg_query_res {
        Ok(q) => q,
        Err(err) => {
            return Box::new(
                future::result(
                    Ok(HttpResponse::NotFound().json(err.to_string()))
                )
            );
        },
    };
    info!("query opts:{:?}", agg_query);

    // TODO: Detect cube based on the query
    let cube = detect_cube(
        req.state().schema.read().unwrap().clone(),
        agg_query.clone()
    );
    let cube = match cube {
        Ok(cube) => cube,
        Err(err) => {
            return Box::new(
                future::result(
                    Ok(HttpResponse::NotFound().json(err.to_string()))
                )
            );
        }
    };
    info!("cube: {:?}", cube);

    // TODO: Should probably refactor this method a bit before it gets much bigger
    // Process year argument (latest/oldest)
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

    // Turn AggregateQueryOpt into Query
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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AggregateQueryOpt {
    drilldowns: Option<Vec<String>>,
    cuts: Option<Vec<String>>,
    measures: Option<Vec<String>>,
    properties: Option<Vec<String>>,
    parents: Option<bool>,
    top: Option<String>,
    top_where: Option<String>,
    sort: Option<String>,
    limit: Option<String>,
    growth: Option<String>,
    rca: Option<String>,
    year: Option<String>,
    debug: Option<bool>,
//    distinct: Option<bool>,
//    nonempty: Option<bool>,
//    sparse: Option<bool>,
}

impl TryFrom<AggregateQueryOpt> for TsQuery {
    type Error = Error;

    fn try_from(agg_query_opt: AggregateQueryOpt) -> Result<Self, Self::Error> {
        let drilldowns: Result<Vec<_>, _> = agg_query_opt.drilldowns
            .map(|ds| {
                ds.iter().map(|d| d.parse()).collect()
            })
            .unwrap_or(Ok(vec![]));

        let cuts: Result<Vec<_>, _> = agg_query_opt.cuts
            .map(|cs| {
                cs.iter().map(|c| c.parse()).collect()
            })
            .unwrap_or(Ok(vec![]));

        let measures: Result<Vec<_>, _> = agg_query_opt.measures
            .map(|ms| {
                ms.iter().map(|m| m.parse()).collect()
            })
            .unwrap_or(Ok(vec![]));

        let properties: Result<Vec<_>, _> = agg_query_opt.properties
            .map(|ms| {
                ms.iter().map(|m| m.parse()).collect()
            })
            .unwrap_or(Ok(vec![]));

        let drilldowns = drilldowns?;
        let cuts = cuts?;
        let measures = measures?;
        let properties = properties?;

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
