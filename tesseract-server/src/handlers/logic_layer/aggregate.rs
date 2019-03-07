use actix_web::{
    FutureResponse,
    HttpRequest,
    HttpResponse,
    Path,
};
use failure::{Error, format_err};
use futures::future::{self};
use lazy_static::lazy_static;
use log::*;
use serde_qs as qs;
use tesseract_core::{Schema};
use tesseract_core::format::{FormatType};
use tesseract_core::names::{LevelName, Measure as MeasureName};

use crate::app::AppState;
use crate::handlers::logic_layer::shared::{LogicLayerQueryOpt, finish_aggregation};


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


/// Performs first step of data aggregation.
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
    lazy_static!{
        static ref QS_NON_STRICT: qs::Config = qs::Config::new(5, false);
    }

    let agg_query_res = QS_NON_STRICT.deserialize_str::<LogicLayerQueryOpt>(&query);
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

    finish_aggregation(req, agg_query, format)
}


// NOTE: Leaving here because we might need this in the future:
///// Detects which cube to use based on the drilldowns, cuts and measures provided.
///// In case the arguments are present in more than one cube, the first cube to match all
///// requirements is returned.
//pub fn detect_cube(schema: Schema, agg_query: LogicLayerQueryOpt) -> Result<String, Error> {
//    let drilldowns = match agg_query.drilldowns {
//        Some(drilldowns) => {
//            let mut d: Vec<LevelName> = vec![];
//            for drilldown in drilldowns {
//                let e: Vec<&str> = drilldown.split(".").collect();
//                let ln = match LevelName::from_vec(e) {
//                    Ok(ln) => ln,
//                    Err(_) => break,
//                };
//                d.push(ln);
//            }
//            d
//        },
//        None => vec![],
//    };
//
//    let measures = match agg_query.measures {
//        Some(measures) => {
//            let mut m: Vec<MeasureName> = vec![];
//            for measure in measures {
//                m.push(MeasureName::new(measure));
//            }
//            m
//        },
//        None => vec![],
//    };
//
//    let result = schema.cubes.iter().filter(|cube| {
//        let level_names = cube.get_all_level_names();
//        let measure_names = cube.get_all_measure_names();
//
//        for drilldown in &drilldowns {
//            if !level_names.contains(drilldown) {
//                break;
//            }
//        }
//
//        for measure in &measures {
//            if !measure_names.contains(measure) {
//                break;
//            }
//        }
//
//        true
//    }).nth(0);
//
//    match result {
//        Some(cube) => Ok(String::from(cube.clone().name)),
//        None => Err(format_err!("No cubes found with the requested drilldowns/measures.")),
//    }
//}
