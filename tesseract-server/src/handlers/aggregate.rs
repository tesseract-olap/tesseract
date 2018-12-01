use actix_web::{
    AsyncResponder,
    FutureResponse,
    HttpResponse,
    Path,
    Query,
    State,
};
use clickhouse_rs::Client as ChClient;
use futures::future::Future;
use log::*;
use serde_derive::{Serialize, Deserialize};

use crate::app::AppState;

pub fn aggregate_handler(
    (state, cube_format, _query): (State<AppState>, Path<(String, String)>, Query<AggregateQueryOpt>)
    ) -> FutureResponse<HttpResponse>
{
    let (cube, format) = cube_format.into_inner();
    info!("cube: {}, format: {}", cube, format);

    // TODO put schema back in later
    let sql = format!("select * from {} limit 10", cube);
    info!("{}", sql);

    // TODO why have to clone?
    ChClient::connect(state.clickhouse_options.clone())
        .and_then(|c| c.ping())
        .and_then(move |c| c.query_all(&sql[..]))
        .from_err()
        .and_then(|(block, _)| {
            info!("Block: {:?}", block);
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

