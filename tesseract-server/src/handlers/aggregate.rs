use actix_web::{
    AsyncResponder,
    FutureResponse,
    HttpRequest,
    HttpResponse,
    Path,
};
use clickhouse_rs::Client as ChClient;
use futures::future::{self, Future};
use lazy_static::lazy_static;
use log::*;
use serde_derive::{Serialize, Deserialize};
use serde_qs as qs;
use std::convert::From;
use tesseract_core::Database;
use tesseract_core::Query as TsQuery;
use tesseract_core::names::{
    Cut,
    Drilldown,
    Measure,
    Property,
};

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
    info!("query opts:{:?}", agg_query);

    // TODO turn AggregateQueryOpt into Query
    // Then write the sql query thing.
    let ts_query: TsQuery = agg_query.into();
    let sql_result = req
        .state()
        .schema
        .sql_query(&cube, &ts_query, Database::Clickhouse);

    let sql = match sql_result {
        Ok(sql) => sql,
        Err(err) => {
            return Box::new(
                future::result(
                    Ok(HttpResponse::NotFound().json(err.to_string()))
                )
            );
        },
    };

    info!("Sql query: {}", sql);

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
    drilldowns: Option<Vec<Drilldown>>,
    cuts: Option<Vec<Cut>>,
    measures: Option<Vec<Measure>>,
    properties: Option<Vec<Property>>,
    parents: Option<bool>,
    debug: Option<bool>,
//    distinct: Option<bool>,
//    nonempty: Option<bool>,
//    sparse: Option<bool>,
}

impl From<AggregateQueryOpt> for TsQuery {
    fn from(agg_query_opt: AggregateQueryOpt) -> Self {
        let drilldowns = agg_query_opt.drilldowns.unwrap_or(vec![]);
        let cuts = agg_query_opt.cuts.unwrap_or(vec![]);
        let measures = agg_query_opt.measures.unwrap_or(vec![]);

        TsQuery {
            drilldowns,
            cuts,
            measures,
        }
    }
}
