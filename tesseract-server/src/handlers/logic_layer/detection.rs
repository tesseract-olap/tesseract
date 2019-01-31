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

/// Returns a Vec<String> of all the dimension name options for a given Cube.
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

/// Returns a Vec<String> of all the measure names for a given Cube.
pub fn get_all_measure_names(cube: Cube) -> Vec<String> {
    let mut measure_names: Vec<String> = vec![];

    for measure in cube.measures {
        measure_names.push(measure.name);
    }

    measure_names
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
