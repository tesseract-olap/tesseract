use actix_web::{
    FutureResponse,
    HttpRequest,
    HttpResponse,
    Path,
};
use futures::future::{self};
use lazy_static::lazy_static;
use log::*;
use serde_qs as qs;
use tesseract_core::format::{FormatType};

use crate::app::AppState;
use crate::handlers::logic_layer::shared::{LogicLayerQueryOpt, finish_aggregation};


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

    finish_aggregation(req, agg_query, cube, format)
}
