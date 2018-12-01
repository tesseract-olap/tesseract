use actix_web::{
    AsyncResponder,
    FutureResponse,
    HttpRequest,
    HttpResponse,
    Path,
};
use clickhouse_rs::Client as ChClient;
use futures::future::Future;
use lazy_static::lazy_static;
use log::*;
use serde_derive::{Serialize, Deserialize};
use serde_qs as qs;

use crate::app::AppState;
use crate::clickhouse::block_to_df;

pub fn aggregate_handler(
    (req, cube_format): (HttpRequest<AppState>, Path<(String, String)>)
    ) -> FutureResponse<HttpResponse>
{
    let (cube, format) = cube_format.into_inner();
    info!("cube: {}, format: {}", cube, format);

    let query = req.query_string();
    lazy_static!{
        static ref QS_NON_STRICT: qs::Config = qs::Config::new(5, false);
    }
    let agg_query = QS_NON_STRICT.deserialize_str::<AggregateQueryOpt>(&query);
    info!("query opts:{:?}", agg_query);

    // TODO put schema back in later
    let sql = format!("select * from {} limit 10", cube);
    info!("{}", sql);

    // TODO why have to clone?
    ChClient::connect(req.state().clickhouse_options.clone())
        .and_then(|c| c.ping())
        .and_then(move |c| c.query_all(&sql[..]))
        .from_err()
        .and_then(|(block, _)| {
            info!("Block: {:?}", block);

            let df = block_to_df(block);
            info!("DF: {:?}", df);

            Ok(HttpResponse::Ok().finish())
            //Ok(_) => Ok(HttpResponse::Ok().finish()),
            //Err(err) => Ok(HttpResponse::NotFound().json(err.to_string())),
        })
        .responder()
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AggregateQueryOpt {
    drilldowns: Option<Vec<String>>,
    cuts: Option<Vec<String>>,
    measures: Option<Vec<String>>,
    properties: Option<Vec<String>>,
    parents: Option<bool>,
    debug: Option<bool>,
//    distinct: Option<bool>,
//    nonempty: Option<bool>,
//    sparse: Option<bool>,
}

