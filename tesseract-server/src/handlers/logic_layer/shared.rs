use actix_web::{
    FutureResponse,
    HttpResponse,
};
use futures::future::{self};
use failure::{Error, format_err, bail};
use std::convert::{TryFrom};
use std::collections::HashMap;

use serde_derive::{Serialize, Deserialize};

use tesseract_core::names::{Cut, Drilldown, Property, Measure, Mask};
use tesseract_core::query::{FilterQuery, GrowthQuery, RcaQuery};
use tesseract_core::Query as TsQuery;
use tesseract_core::schema::{Cube};

use crate::logic_layer::LogicLayerConfig;


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


#[derive(Debug, Clone, Deserialize)]
pub struct LogicLayerQueryOpt {
    pub cube_obj: Option<Cube>,
    pub config: Option<LogicLayerConfig>,

    pub cube: String,
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

        let mut captions: Vec<Property> = vec![];
        let locales: Vec<String> = match agg_query_opt.locale {
            Some(l) => l.split(",").map(|s| s.to_string()).collect(),
            None => vec![]
        };

        // Moving this out of the cut resolution because drills will need to
        // insert to this hashmap in case it receives a named set value.
        let mut agg_query_opt_cuts = match agg_query_opt.cuts {
            Some(c) => c.clone(),
            None => HashMap::new()
        };

        let ll_config = agg_query_opt.config.clone();

        let drilldowns: Vec<_> = agg_query_opt.drilldowns
            .map(|ds| {
                let mut drilldowns: Vec<Drilldown> = vec![];

                for l in LogicLayerQueryOpt::deserialize_args(ds) {
                    // Check logic layer config for any drill substitutions
                    let drill_value = match ll_config.clone() {
                        Some(llc) => {
                            match llc.substitute_drill_value(l.clone()) {
                                Some(ln) => {
                                    agg_query_opt_cuts.entry(ln.clone()).or_insert(l.clone());
                                    ln
                                },
                                None => l.clone()
                            }
                        },
                        None => l.clone()
                    };

                    let (dimension, hierarchy, level) = match cube.identify_level(drill_value.clone()) {
                        Ok(dhl) => dhl,
                        Err(_) => break
                    };
                    let d = Drilldown::new(
                        dimension.clone(), hierarchy.clone(), drill_value.clone()
                    );
                    drilldowns.push(d);

                    // Check for captions for this level
                    if let Some(props) = level.properties {
                        for prop in props {
                            if let Some(cap) = prop.caption_set {
                                for locale in locales.clone() {
                                    if locale == cap {
                                        captions.push(
                                            Property::new(
                                                dimension.clone(),
                                                hierarchy.clone(),
                                                l.clone(), prop.name.clone()
                                            )
                                        )
                                    }
                                }
                            } else {
                                continue
                            }
                        }
                    } else {
                        continue
                    }
                }

                drilldowns
            })
            .unwrap_or(vec![]);

        let mut cuts: Vec<Cut > = vec![];
        for (level_name, cut) in agg_query_opt_cuts.iter() {
            if cut.is_empty() {
                continue;
            }

            // Check logic layer config for any cut substitutions
            let cut_value = match ll_config.clone() {
                Some(llc) => {
                    llc.substitute_cut(level_name.clone(), cut.clone())
                },
                None => cut.clone()
            };

            let (dimension, hierarchy, level) = match cube.identify_level(level_name.to_string()) {
                Ok(dh) => dh,
                Err(_) => continue
            };

            let c = Cut::new(
                dimension.clone(), hierarchy.clone(),
                level_name.clone(),
                cut_value.split(",").map(|s| s.to_string()).collect(),
                Mask::Include, false
            );

            cuts.push(c);
        }

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
                    let p = Property::new(
                        dimension.clone(), hierarchy.clone(),
                        level.clone(), property.clone()
                    );
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

        let growth = match agg_query_opt.growth {
            Some(g) => {
                let gro_split: Vec<String> = g.split(",").map(|s| s.to_string()).collect();

                if gro_split.len() == 1 {
                    return Err(format_err!("Please provide a growth measure name."));
                } else if gro_split.len() != 2 {
                    return Err(format_err!("Bad formatting for growth param."));
                }

                let level = gro_split[0].clone();
                let measure = gro_split[1].clone();

                let (dimension, hierarchy, _) = match cube.identify_level(level.clone()) {
                    Ok(dh) => dh,
                    Err(_) => return Err(format_err!("Unable to identify growth level."))
                };

                let growth = GrowthQuery::new(dimension, hierarchy, level, measure);

                Some(growth)
            },
            None => None
        };

        let rca = match agg_query_opt.rca {
            Some(r) => {
                let rca_split: Vec<String> = r.split(",").map(|s| s.to_string()).collect();

                if rca_split.len() <= 2 || rca_split.len() >= 4 {
                    return Err(format_err!("Bad formatting for RCA param."));
                }

                let drill1_l = rca_split[0].clone();
                let drill2_l = rca_split[1].clone();
                let measure = rca_split[2].clone();

                let (drill1_d, drill1_h, _) = match cube.identify_level(drill1_l.clone()) {
                    Ok(dh) => dh,
                    Err(_) => return Err(format_err!("Unable to identify RCA drilldown #1 level."))
                };

                let (drill2_d, drill2_h, _) = match cube.identify_level(drill2_l.clone()) {
                    Ok(dh) => dh,
                    Err(_) => return Err(format_err!("Unable to identify RCA drilldown #2 level."))
                };

                let rca = RcaQuery::new(
                    drill1_d, drill1_h, drill1_l,
                    drill2_d, drill2_h, drill2_l,
                    measure
                );

                Some(rca)
            },
            None => None
        };

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
