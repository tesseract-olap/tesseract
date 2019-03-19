use actix_web::{
    FutureResponse,
    HttpResponse,
};
use futures::future::{self};
use failure::{Error, format_err, bail};
use std::convert::{TryFrom};
use std::collections::HashMap;

use serde_derive::{Serialize, Deserialize};

use tesseract_core::names::{Cut, Drilldown, Property, Measure};
use tesseract_core::query::{FilterQuery};
use tesseract_core::Query as TsQuery;
use tesseract_core::schema::{Cube};


#[derive(Debug, Clone)]
pub enum TimeValue {
    First,
    Last,
    Value(u32),
}

impl TimeValue {
    pub fn from_str(raw: String) -> Result<Self, Error> {
        if raw == "latest" {
            Ok(TimeValue::Last)
        } else if raw == "oldest" {
            Ok(TimeValue::First)
        } else {
            match raw.parse::<u32>() {
                Ok(n) => Ok(TimeValue::Value(n)),
                Err(_) => Err(format_err!("Wrong type for time argument."))
            }
        }
    }
}


#[derive(Debug, Clone)]
pub enum TimePrecision {
    Year,
    Quarter,
    Month,
    Week,
    Day,
}

impl TimePrecision {
    pub fn from_str(raw: String) -> Result<Self, Error> {
        match raw.as_ref() {
            "year" => Ok(TimePrecision::Year),
            "quarter" => Ok(TimePrecision::Quarter),
            "month" => Ok(TimePrecision::Month),
            "week" => Ok(TimePrecision::Week),
            "day" => Ok(TimePrecision::Day),
            _ => Err(format_err!("Wrong type for time precision argument."))
        }
    }
}


#[derive(Debug, Clone)]
pub struct Time {
    pub precision: TimePrecision,
    pub value: TimeValue,
}

impl Time {
    pub fn from_str(raw: String) -> Result<Self, Error> {
        let e: Vec<&str> = raw.split(".").collect();

        if e.len() != 2 {
            return Err(format_err!("Wrong format for time argument."));
        }

        let precision = match TimePrecision::from_str(e[0].to_string()) {
            Ok(precision) => precision,
            Err(err) => return Err(err),
        };
        let value = match TimeValue::from_str(e[1].to_string()) {
            Ok(value) => value,
            Err(err) => return Err(err),
        };

        Ok(Time {precision, value})
    }

    pub fn from_key_value(key: String, value: String) -> Result<Self, Error> {
        let precision = match TimePrecision::from_str( key) {
            Ok(precision) => precision,
            Err(err) => return Err(err),
        };
        let value = match TimeValue::from_str(value) {
            Ok(value) => value,
            Err(err) => return Err(err),
        };

        Ok(Time {precision, value})
    }
}


/// Helper method to return errors (FutureResponse<HttpResponse>).
pub fn boxed_error(message: String) -> FutureResponse<HttpResponse> {
    Box::new(
        future::result(
            Ok(HttpResponse::NotFound().json(message))
        )
    )
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicLayerQueryOpt {
    pub cube: String,
    pub cube_obj: Option<Cube>,

    pub drilldowns: Option<String>,
    #[serde(flatten)]
    pub cuts: Option<HashMap<String, String>>,
    pub time: Option<String>,
    measures: Option<String>,
    properties: Option<String>,
    filters: Option<String>,
    parents: Option<bool>,
    top: Option<String>,
    top_where: Option<String>,
    sort: Option<String>,
    limit: Option<String>,
    growth: Option<String>,
    rca: Option<String>,
    debug: Option<bool>,
    locale: Option<String>,
//    distinct: Option<bool>,
//    nonempty: Option<bool>,
//    sparse: Option<bool>,
}

impl LogicLayerQueryOpt {
    pub fn deserialize_args(arg: String) -> Vec<String> {
        let mut open = false;
        let mut curr_str = "".to_string();
        let mut arg_vec: Vec<String> = vec![];

        for c in arg.chars() {
            let c_str = c.to_string();
            if c_str == "[" {
                open = true;
            } else if c_str == "]" {
                arg_vec.push(curr_str.clone());
                curr_str = "".to_string();
                open = false;
            } else if c_str == "," {
                if open {
                    curr_str += &c_str;
                } else {
                    arg_vec.push(curr_str.clone());
                    curr_str = "".to_string();
                }
            } else {
                curr_str += &c_str;
            }
        }

        if curr_str.len() >= 1 {
            arg_vec.push(curr_str.clone());
        }

        arg_vec
    }
}

impl TryFrom<LogicLayerQueryOpt> for TsQuery {
    type Error = Error;

    fn try_from(agg_query_opt: LogicLayerQueryOpt) -> Result<Self, Self::Error> {
        let cube = match agg_query_opt.cube_obj {
            Some(c) => c,
            None => bail!("No cubes found with the given name")
        };

        let mut caption_strings: Vec<String> = vec![];
        let locales: Vec<String> = match agg_query_opt.locale {
            Some(l) => l.split(",").map(|s| s.to_string()).collect(),
            None => vec![]
        };

        let drilldowns: Vec<_> = agg_query_opt.drilldowns
            .map(|ds| {
                let mut drilldowns: Vec<Drilldown> = vec![];

                for l in LogicLayerQueryOpt::deserialize_args(ds) {
                    let (dimension, hierarchy, level) = match cube.identify_level(l.clone()) {
                        Ok(dhl) => dhl,
                        Err(_) => break
                    };
                    let d = match format!("[{}].[{}].[{}]", dimension, hierarchy, l).parse() {
                        Ok(d) => d,
                        Err(_) => break
                    };
                    drilldowns.push(d);

                    // Check for captions for this drilldown
                    match level.properties {
                        Some(props) => {
                            for prop in props {
                                match prop.caption_set {
                                    Some(cap) => {
                                        for locale in locales.clone() {
                                            if locale == cap {
                                                caption_strings.push(
                                                    format!("[{}].[{}].[{}].[{}]", dimension, hierarchy, l, prop.name)
                                                );
                                            }
                                        }
                                    },
                                    None => continue
                                }
                            }
                        },
                        None => continue
                    }
                }

                drilldowns
            })
            .unwrap_or(vec![]);

        let cuts: Vec<_> = match agg_query_opt.cuts {
            Some(cs) => {
                let mut cuts: Vec<Cut> = vec![];

                for (cut, cut_value) in cs.iter() {
                    if cut_value.is_empty() {
                        continue;
                    }

                    let (dimension, hierarchy, level) = match cube.identify_level(cut.to_string()) {
                        Ok(dh) => dh,
                        Err(_) => continue
                    };
                    let c = match format!("[{}].[{}].[{}].[{}]", dimension, hierarchy, cut, cut_value).parse() {
                        Ok(c) => c,
                        Err(_) => continue
                    };
                    cuts.push(c);

                    // Check for captions for this cut
                    match level.properties {
                        Some(props) => {
                            for prop in props {
                                match prop.caption_set {
                                    Some(cap) => {
                                        for locale in locales.clone() {
                                            if locale == cap {
                                                caption_strings.push(
                                                    format!("[{}].[{}].[{}].[{}]", dimension, hierarchy, level.name, prop.name)
                                                );
                                            }
                                        }
                                    },
                                    None => continue
                                }
                            }
                        },
                        None => continue
                    }
                }

                cuts
            },
            None => vec![]
        };

        let measures: Vec<_> = agg_query_opt.measures
            .map(|ms| {
                let mut measures: Vec<Measure> = vec![];

                for measure in LogicLayerQueryOpt::deserialize_args(ms) {
                    let m = match measure.parse() {
                        Ok(m) => m,
                        Err(_) => break
                    };
                    measures.push(m);
                }

                measures
            })
            .unwrap_or(vec![]);

        let properties: Vec<_> = agg_query_opt.properties
            .map(|ps| {
                let mut properties: Vec<Property> = vec![];

                for property in LogicLayerQueryOpt::deserialize_args(ps) {
                    let (dimension, hierarchy, level) = match cube.identify_property(property.clone()) {
                        Ok(dhl) => dhl,
                        Err(_) => break
                    };
                    let p = match format!("[{}].[{}].[{}].[{}]", dimension, hierarchy, level, property).parse() {
                        Ok(p) => p,
                        Err(_) => break
                    };
                    properties.push(p);
                }

                properties
            })
            .unwrap_or(vec![]);

        // TODO: Implement
        let filters: Vec<FilterQuery>= vec![];

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

        let mut captions: Vec<Property> = vec![];
        for cap in caption_strings {
            match cap.parse() {
                Ok(c) => captions.push(c),
                Err(_) => continue
            }
        }

        let debug = agg_query_opt.debug.unwrap_or(false);

        Ok(TsQuery {
            drilldowns,
            cuts,
            measures,
            parents,
            properties,
            captions,
            top,
            top_where,
            sort,
            limit,
            rca,
            growth,
            debug,
            filters,
        })
    }
}
