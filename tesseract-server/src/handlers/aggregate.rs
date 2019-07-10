use actix_web::{
    HttpRequest,
    HttpResponse,
    web::Path,
};
use failure::Error;
use futures::future::{self, Future, Either};
use lazy_static::lazy_static;
use log::*;
use serde_derive::{Serialize, Deserialize};
use serde_qs as qs;
use std::convert::{TryFrom, TryInto};
use tesseract_core::format::{format_records, FormatType};
use tesseract_core::Query as TsQuery;

use crate::app::AppState;
use crate::errors::ServerError;
use super::util;

/// Handles default aggregation when a format is not specified.
/// Default format is CSV.
pub fn aggregate_default_handler(req: HttpRequest, cube: Path<String>) -> impl Future<Item=HttpResponse, Error=Error>
{
    let cube_format = (cube.into_inner(), "csv".to_owned());
    do_aggregate(req, cube_format)
}

/// Handles aggregation when a format is specified.
pub fn aggregate_handler(req: HttpRequest, cube_format: Path<(String, String)>) -> impl Future<Item=HttpResponse, Error=Error>
{
    do_aggregate(req, cube_format.into_inner())
}

/// Performs data aggregation.
pub fn do_aggregate(
    req: HttpRequest,
    cube_format: (String, String),
    ) -> impl Future<Item=HttpResponse, Error=Error>
{
    let (cube, format) = cube_format;
    let app_state = req.app_data::<AppState>().unwrap();

    let format = format.parse::<FormatType>();
    let format = match format {
        Ok(f) => f,
        Err(err) => {
            return Either::A(
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
            return Either::A(
                future::result(
                    Ok(HttpResponse::NotFound().json(err.to_string()))
                )
            );
        },
    };
    info!("query opts:{:?}", agg_query);

    // Turn AggregateQueryOpt into Query
    let ts_query: Result<TsQuery, _> = agg_query.try_into();
    let ts_query = match ts_query {
        Ok(q) => q,
        Err(err) => {
            return Either::A(
                future::result(
                    Ok(HttpResponse::NotFound().json(err.to_string()))
                )
            );
        },
    };

    let query_ir_headers = app_state
        .schema.read().unwrap()
        .sql_query(&cube, &ts_query);

    let (query_ir, headers) = match query_ir_headers {
        Ok(x) => x,
        Err(err) => {
            return Either::A(
                future::result(
                    Ok(HttpResponse::NotFound().json(err.to_string()))
                )
            );
        },
    };

    let sql = app_state
        .backend
        .generate_sql(query_ir);

    info!("Sql query: {}", sql);
    info!("Headers: {:?}", headers);
    let debug_mode = app_state.debug;
    let result = app_state
        .backend
        .exec_sql(sql)
        .and_then(move |df| {
            let content_type = util::format_to_content_type(&format);

            match format_records(&headers, df, format) {
                Ok(res) => {
                    Ok(HttpResponse::Ok()
                        .set(content_type)
                        .body(res))
                },
                Err(err) => Ok(HttpResponse::NotFound().json(err.to_string())),
            }
        })
        .map_err(move |e| {
            if debug_mode {
                ServerError::Db { cause: e.to_string() }.into()
            } else {
                ServerError::Db { cause: "Internal Server Error 1010".to_owned() }.into()
            }
        });
    Either::B(result)
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AggregateQueryOpt {
    drilldowns: Option<Vec<String>>,
    cuts: Option<Vec<String>>,
    measures: Option<Vec<String>>,
    properties: Option<Vec<String>>,
    filters: Option<Vec<String>>,
    captions: Option<Vec<String>>,
    parents: Option<bool>,
    top: Option<String>,
    top_where: Option<String>,
    sort: Option<String>,
    limit: Option<String>,
    growth: Option<String>,
    rca: Option<String>,
    rate: Option<String>,
    debug: Option<bool>,
    exclude_default_members: Option<bool>,
//    distinct: Option<bool>,
//    nonempty: Option<bool>,
    sparse: Option<bool>,
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

        let filters: Result<Vec<_>, _> = agg_query_opt.filters
            .map(|fs| {
                fs.iter().map(|f| f.parse()).collect()
            })
            .unwrap_or(Ok(vec![]));

        let captions: Result<Vec<_>, _> = agg_query_opt.captions
            .map(|cs| {
                cs.iter().map(|c| c.parse()).collect()
            })
            .unwrap_or(Ok(vec![]));

        let drilldowns = drilldowns?;
        let cuts = cuts?;
        let measures = measures?;
        let properties = properties?;
        let filters = filters?;
        let captions = captions?;

        let parents = agg_query_opt.parents.unwrap_or(false);

        let top = agg_query_opt.top
            .map(|t| t.parse())
            .transpose()?;
        let top_where = agg_query_opt.top_where
            .map(|t| t.parse())
            .transpose()?;
        let sort = agg_query_opt.sort
            .map(|s| s.parse())
            .transpose()?;
        let limit = agg_query_opt.limit
            .map(|l| l.parse())
            .transpose()?;

        let growth = agg_query_opt.growth
            .map(|g| g.parse())
            .transpose()?;

        let rca = agg_query_opt.rca
            .map(|r| r.parse())
            .transpose()?;

        let rate = agg_query_opt.rate
            .map(|r| r.parse())
            .transpose()?;

        let debug = agg_query_opt.debug.unwrap_or(false);
        let sparse = agg_query_opt.sparse.unwrap_or(false);
        let exclude_default_members = agg_query_opt.exclude_default_members.unwrap_or(false);

        // TODO: deserialize rate
        Ok(TsQuery {
            drilldowns,
            cuts,
            measures,
            parents,
            properties,
            filters,
            captions,
            top,
            top_where,
            sort,
            limit,
            rca,
            growth,
            debug,
            rate,
            sparse,
            exclude_default_members,
        })
    }
}
