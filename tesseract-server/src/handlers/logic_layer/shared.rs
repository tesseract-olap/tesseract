use actix_web::{
    AsyncResponder,
    FutureResponse,
    HttpRequest,
    HttpResponse,
};
use failure::{Error, format_err};
use futures::future::{self, Future};
use log::*;
use serde_derive::{Serialize, Deserialize};
use std::convert::{TryFrom, TryInto};

use tesseract_core::format::{format_records, FormatType};
use tesseract_core::Query as TsQuery;

use crate::app::AppState;


#[derive(Debug, Clone)]
pub enum Year {
    First,
    Last,
    Value(u32),
}

impl Year {
    pub fn from_str(raw: String) -> Result<Self, Error> {
        if raw == "latest" {
            Ok(Year::Last)
        } else if raw == "oldest" {
            Ok(Year::First)
        } else {
            match raw.parse::<u32>() {
                Ok(n) => Ok(Year::Value(n)),
                Err(_) => Err(format_err!("Wrong type for year argument."))
            }
        }
    }
}

/// The last few aggregation operations are common across all different routes.
/// This method implements that step to avoid duplication.
pub fn finish_aggregation(
    req: HttpRequest<AppState>,
    mut agg_query: LogicLayerQueryOpt,
    cube: String,
    format: FormatType
) -> FutureResponse<HttpResponse> {
    // Process `year` param (latest/oldest)
    match &agg_query.year {
        Some(s) => {
            let cube_info = req.state().cache.read().unwrap().find_cube_info(&cube);

            // TODO: Transform this year string into a `Year` enum
            let year = match Year::from_str(s.clone()) {
                Ok(year) => year,
                Err(err) => {
                    return Box::new(
                        future::result(
                            Ok(HttpResponse::NotFound().json(err.to_string()))
                        )
                    );
                },
            };
            println!("HHHHHHH: {:?}", year);

            match cube_info {
                Some(info) => {
                    let cut = match info.get_year_cut(s.to_string()) {
                        Ok(cut) => cut,
                        Err(err) => {
                            return Box::new(
                                future::result(
                                    Ok(HttpResponse::NotFound().json(err.to_string()))
                                )
                            );
                        }
                    };

                    agg_query.cuts = match agg_query.cuts {
                        Some(mut cuts) => {
                            cuts.push(cut);
                            Some(cuts)
                        },
                        None => Some(vec![cut]),
                    }
                },
                None => (),
            };
        },
        None => (),
    }
    info!("query opts:{:?}", agg_query);

    // Turn AggregateQueryOpt into TsQuery
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

    let query_ir_headers = req
        .state()
        .schema.read().unwrap()
        .sql_query(&cube, &ts_query);

    let (query_ir, headers) = match query_ir_headers {
        Ok(x) => x,
        Err(err) => {
            return Box::new(
                future::result(
                    Ok(HttpResponse::NotFound().json(err.to_string()))
                )
            );
        },
    };

    let sql = req.state()
        .backend
        .generate_sql(query_ir);

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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LogicLayerQueryOpt {
    pub drilldowns: Option<Vec<String>>,
    pub cuts: Option<Vec<String>>,
    pub measures: Option<Vec<String>>,
    pub year: Option<String>,
    properties: Option<Vec<String>>,
    parents: Option<bool>,
    top: Option<String>,
    top_where: Option<String>,
    sort: Option<String>,
    limit: Option<String>,
    growth: Option<String>,
    rca: Option<String>,
    debug: Option<bool>,
//    distinct: Option<bool>,
//    nonempty: Option<bool>,
//    sparse: Option<bool>,
}

impl TryFrom<LogicLayerQueryOpt> for TsQuery {
    type Error = Error;

    fn try_from(agg_query_opt: LogicLayerQueryOpt) -> Result<Self, Self::Error> {
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
        let properties = properties?;

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

        let debug = agg_query_opt.debug.unwrap_or(false);

        Ok(TsQuery {
            drilldowns,
            cuts,
            measures,
            parents,
            properties,
            top,
            top_where,
            sort,
            limit,
            rca,
            growth,
            debug,
        })
    }
}
