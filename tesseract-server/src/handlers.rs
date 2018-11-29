use actix_web::{
    AsyncResponder,
    FutureResponse,
    HttpRequest,
    HttpResponse,
    Path,
    Query,
    Result as ActixResult,
    State,
};
use clickhouse_rs::Client as ChClient;
use futures::future::Future;
use log::*;
use serde_derive::{Serialize, Deserialize};

use crate::app::AppState;

pub fn index_handler(_req: HttpRequest<AppState>) -> ActixResult<HttpResponse> {
    Ok(HttpResponse::Ok().json(
        Status {
            status: "ok".to_owned(),
            // TODO set this as the Cargo.toml version, after structopt added
            version: "0.1.0".to_owned(),
        }
    ))
}

#[derive(Debug, Serialize)]
struct Status {
    status: String,
    version: String,
}

pub fn test_handler(
    (state, schema_table, _query): (State<AppState>, Path<(String, String)>, Query<AggregateQueryOpt>)
    ) -> FutureResponse<HttpResponse>
{
    let (schema, table) = schema_table.into_inner();
    info!("schema: {}, table: {}", schema, table);

    // TODO put schema back in later
    let sql = format!("select * from {} limit 10", table);
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

#[derive(Debug, Deserialize, Serialize)]
pub struct FlushQueryOpt {
    pub secret: String,
}

