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
use crate::handlers::logic_layer::aggregate::{finish_aggregation, AggregateQueryOpt};


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

/// Detects which cube to use based on the drilldowns, cuts and measures provided.
/// In case the arguments are present in more than one cube, the first cube to match all
/// requirements is returned.
pub fn detect_cube(schema: Schema, agg_query: AggregateQueryOpt) -> Result<String, Error> {
    let drilldowns = match agg_query.drilldowns {
        Some(drilldowns) => {
            let mut d: Vec<String> = vec![];
            for drilldown in drilldowns {
                let e: Vec<&str> = drilldown.split(".").collect();
                let mut final_drilldown = String::from("");
                if e.len() == 2 {
                    final_drilldown = format!("{}.{}.{}", e[0], e[0], e[1]).to_string();
                } else if e.len() == 3 {
                    final_drilldown = drilldown;
                } else {
                    return Err(format_err!("Wrong drilldown format. Make sure your drilldown names are correct."));
                }
                d.push(final_drilldown);
            }
            d
        },
        None => vec![],
    };

//    // TODO: Remove anything after the level
//    let cuts = match agg_query.cuts {
//        Some(cuts) => cuts,
//        None => vec![],
//    };

    let measures = match agg_query.measures {
        Some(measures) => measures,
        None => vec![],
    };

    // TODO: Avoid clone here?
    for cube in schema.cubes {
        let dimension_names = cube.get_all_dimension_names();
        let measure_names = cube.get_all_measure_names();

        // If this is true, we already know this is not the right cube, so need
        // to continue to next iteration of the loop
        let mut exit = false;

        for drilldown in &drilldowns {
            if !dimension_names.contains(drilldown) {
                exit = true;
                break;
            }
        }

        if exit {
            continue;
        }

//        for cut in &cuts {
//            if !dimension_names.contains(cut) {
//                break;
//            }
//        }

        for measure in &measures {
            if !measure_names.contains(measure) {
                break;
            }
        }

        return Ok(String::from(cube.name));
    }

    Err(format_err!("No cubes found with the requested drilldowns/cuts/measures."))
}

/// Performs first step of data aggregation, including cube detection.
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

    // Detect cube based on the query
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

    finish_aggregation(req, agg_query, cube, format)
}
