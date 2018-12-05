use actix_web::{
    AsyncResponder,
    FutureResponse,
    HttpRequest,
    HttpResponse,
    Path,
};
use clickhouse_rs::Client as ChClient;
use failure::Error;
use futures::future::{self, Future};
use lazy_static::lazy_static;
use log::*;
use serde_derive::{Serialize, Deserialize};
use serde_qs as qs;
use std::convert::{TryFrom, TryInto};
use std::time::Instant;
use tesseract_core::Database;
use tesseract_core::Query as TsQuery;

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

    let time_start = Instant::now();
    ChClient::connect(req.state().clickhouse_options.clone())
        .and_then(|c| c.ping())
        .and_then(move |c| c.query_all(&sql[..]))
        .from_err()
        .and_then(move |(block, _)| {
            let timing = time_start.elapsed();
            info!("Time for sql execution: {}.{}", timing.as_secs(), timing.subsec_millis());
            //info!("Block: {:?}", block);

            let df = block_to_df(block);
            //info!("DF: {:?}", df);

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

        let drilldowns = drilldowns?;
        let cuts = cuts?;
        let measures = measures?;
        let parents = agg_query_opt.parents.unwrap_or(false);

        Ok(TsQuery {
            drilldowns,
            cuts,
            measures,
            parents,
        })
    }
}
