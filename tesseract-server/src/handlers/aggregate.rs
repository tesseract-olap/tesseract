use actix_web::{
    AsyncResponder,
    FutureResponse,
    HttpRequest,
    HttpResponse,
    Path,
};
use failure::Error;
use futures::future::{self, Future};
use lazy_static::lazy_static;
use log::*;
use serde_derive::{Serialize, Deserialize};
use serde_qs as qs;
use std::convert::{TryFrom, TryInto};
use tesseract_core::format::{format_records, FormatType};
use tesseract_core::Database;
use tesseract_core::Query as TsQuery;

use crate::app::AppState;

pub fn aggregate_default_handler(
    (req, cube): (HttpRequest<AppState>, Path<String>)
    ) -> FutureResponse<HttpResponse>
{
    let cube_format = (cube.into_inner(), "csv".to_owned());
    do_aggregate(req, cube_format)
}

pub fn aggregate_handler(
    (req, cube_format): (HttpRequest<AppState>, Path<(String, String)>)
    ) -> FutureResponse<HttpResponse>
{
    do_aggregate(req, cube_format.into_inner())
}

pub fn do_aggregate(
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

    let (sql, headers) = match sql_result {
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

#[derive(Debug, Deserialize, Serialize)]
pub struct AggregateQueryOpt {
    drilldowns: Option<Vec<String>>,
    cuts: Option<Vec<String>>,
    measures: Option<Vec<String>>,
    properties: Option<Vec<String>>,
    parents: Option<bool>,
    top: Option<String>,
    sort: Option<String>,
    limit: Option<String>,
//    debug: Option<bool>,
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
        let parents = agg_query_opt.parents.unwrap_or(false);
        let properties = properties?;

        let top = agg_query_opt.top
            .map(|t| t.parse())
            .transpose()?;
        let sort = agg_query_opt.sort
            .map(|s| s.parse())
            .transpose()?;
        let limit = agg_query_opt.limit
            .map(|l| l.parse())
            .transpose()?;

        Ok(TsQuery {
            drilldowns,
            cuts,
            measures,
            parents,
            properties,
            top,
            sort,
            limit,
        })
    }
}
