use actix_web::{
    FutureResponse,
    HttpResponse,
};
use futures::future::{self};
use failure::{Error, format_err, bail};
use std::convert::{TryFrom};
use std::collections::HashMap;

use serde_derive::Deserialize;

use tesseract_core::names::{Cut, Drilldown, Property, Measure};
use tesseract_core::query::{FilterQuery, GrowthQuery, RcaQuery, TopQuery, RateQuery};
use tesseract_core::{Query as TsQuery, Schema, MeaOrCalc};
use tesseract_core::schema::{Cube};

use crate::logic_layer::{LogicLayerConfig, CubeCache};


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
    pub cube_cache: Option<CubeCache>,
    pub config: Option<LogicLayerConfig>,
    pub schema: Option<Schema>,

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
    exclude_default_members: Option<bool>,
    locale: Option<String>,
//    distinct: Option<bool>,
//    nonempty: Option<bool>,
    sparse: Option<bool>,
    rate: Option<String>,
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
        let cube = match agg_query_opt.clone().cube_obj {
            Some(c) => c,
            None => bail!("No cubes found with the given name")
        };

        let level_map = match agg_query_opt.clone().cube_cache {
            Some(cc) => cc.level_map,
            None => bail!("Unable to construct unique level name map")
        };

        let property_map = match agg_query_opt.clone().cube_cache {
            Some(cc) => cc.property_map,
            None => bail!("Unable to construct unique property name map")
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

        let parents = agg_query_opt.parents.unwrap_or(false);

        let mut drilldowns: Vec<_> = agg_query_opt.drilldowns
            .map(|ds| {
                let mut drilldowns: Vec<Drilldown> = vec![];

                for level_value in LogicLayerQueryOpt::deserialize_args(ds) {
                    // Check logic layer config for any named set substitutions
                    let level_key = match ll_config.clone() {
                        Some(ll_conf) => {
                            match ll_conf.substitute_drill_value(level_value.clone()) {
                                Some(ln) => {
                                    agg_query_opt_cuts
                                        .entry(ln.clone())
                                        .or_insert(level_value.clone());
                                    ln
                                },
                                None => level_value.clone()
                            }
                        },
                        None => level_value.clone()
                    };

                    let level_name = match level_map.get(&level_key) {
                        Some(l) => l,
                        None => break
                    };

                    let level = match cube.get_level(level_name) {
                        Some(l) => l,
                        None => break
                    };

                    let drilldown = Drilldown::new(
                        level_name.dimension.clone(),
                        level_name.hierarchy.clone(),
                        level_name.level.clone()
                    );

                    drilldowns.push(drilldown);

                    // Check for captions for this level
                    let new_captions = level.get_captions(&level_name, &locales);
                    captions.extend_from_slice(&new_captions);

                    // if parents, check captions for parent levels
                    // Same logic as above, for checking captions for a level
                    if parents {
                        let level_parents = cube.get_level_parents(level_name).unwrap_or(vec![]);
                        for parent_level in level_parents {
                            if let Some(ref props) = parent_level.properties {
                                for prop in props {
                                    if let Some(ref cap) = prop.caption_set {
                                        for locale in &locales {
                                            if locale == cap {
                                                captions.push(
                                                    Property::new(
                                                        level_name.dimension.clone(),
                                                        level_name.hierarchy.clone(),
                                                        parent_level.name.clone(),
                                                        prop.name.clone(),
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
                    }
                }

                drilldowns
            })
            .unwrap_or(vec![]);

        let mut cuts: Vec<Cut> = vec![];
        for (level_key, cut_value) in agg_query_opt_cuts.iter() {
            if cut_value.is_empty() {
                continue;
            }

            // Check logic layer config for any cut substitutions
            let cut_val = match ll_config.clone() {
                Some(ll_conf) => {
                    ll_conf.substitute_cut(level_key.clone(), cut_value.clone())
                },
                None => cut_value.clone()
            };

            // Identify members and special operations
            let supported_operations = vec![
                "parent".to_string(),
                "children".to_string(),
                "neighbors".to_string()
            ];

            let members: Vec<String> = cut_val.split(",").map(|s| s.to_string()).collect();
            let mut final_members: Vec<String> = vec![];
            let mut operation: Option<String> = None;

            for member in &members {
                if supported_operations.contains(&member) {
                    // Check that no supported operations have been set yet
                    if operation.is_some() {
                        return Err(format_err!("Multiple cut operations are not supported."));
                    } else {
                        operation = Some(member.clone());
                    }
                } else {
                    final_members.push(member.clone());
                }
            }

            let level_name = match level_map.get(level_key) {
                Some(l) => l,
                None => break
            };

            let level = match cube.get_level(level_name) {
                Some(l) => l,
                None => break
            };

            match operation {
                Some(operation) => {
                    if operation == "children".to_string() {

                        // Country=1,parent
                        // - 1. Identify Country as the child level
                        // - 2. Add Country drilldown
                        // - 3. Keep Continent cut
                        // - 4. Might also need two queries here
                        return Err(format_err!("`{}` operation not currently supported.", operation));

                    } else if operation == "parent".to_string() {

                        // Continent=1,children
                        // - 1. Identify Continent as the parent level
                        // - 2. Somehow find EXACTLY which continent is the parent of this country
                        // - 3. Add a cut for that Continent:
                        //   - e.g. Continent=1
                        // - 4. Perform two queries and join the result
                        return Err(format_err!("`{}` operation not currently supported.", operation));

                    } else if operation == "neighbors".to_string() {

                        if level_name.level == "Geography".to_string() {
                            // TODO: Add a better way to identify geography levels in the schema
                            // TODO: Wait for geoservice API
                            return Err(format_err!("Geoservice not currently supported."));
                        } else {
                            let drilldown = Drilldown::new(
                                level_name.dimension.clone(),
                                level_name.hierarchy.clone(),
                                level_name.level.clone()
                            );

                            drilldowns.push(drilldown);

                            // Add captions for this level
                            let new_captions = level.get_captions(&level_name, &locales);
                            captions.extend_from_slice(&new_captions);
                        }

                    } else {
                        return Err(format_err!("Unrecognized operation: `{}`.", operation));
                    }
                },
                None => {
                    let (mask, for_match, cut_val) = Cut::parse_cut(&cut_val);

                    let cut = Cut::new(
                        level_name.dimension.clone(),
                        level_name.hierarchy.clone(),
                        level_name.level.clone(),
                        final_members, mask, for_match
                    );

                    cuts.push(cut);
                }
            }
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

                for property_value in LogicLayerQueryOpt::deserialize_args(ps) {
                    // TODO: Break or bail?
                    let property = match property_map.get(&property_value) {
                        Some(p) => p,
                        None => break
                    };

                    properties.push(property.clone());
                }

                properties
            })
            .unwrap_or(vec![]);

        // TODO: Implement
        let filters: Vec<FilterQuery>= vec![];

        let top: Option<TopQuery> = agg_query_opt.top.clone()
            .map(|t| {
                let top_split: Vec<String> = t.split(",").map(|s| s.to_string()).collect();

                let level_name = match level_map.get(&top_split[1]) {
                    Some(l) => l,
                    None => bail!("Unable to find top level")
                };

                let mea_or_calc: MeaOrCalc = top_split[2].parse()?;

                Ok(TopQuery::new(
                    top_split[0].parse()?,
                    level_name.clone(),
                    vec![mea_or_calc],
                    top_split[3].parse()?
                ))
            })
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

                let level_key = gro_split[0].clone();
                let measure = gro_split[1].clone();

                let level_name = match level_map.get(&level_key) {
                    Some(l) => l,
                    None => bail!("Unable to find growth level")
                };

                let growth = GrowthQuery::new(
                    level_name.dimension.clone(),
                    level_name.hierarchy.clone(),
                    level_name.level.clone(),
                    measure
                );

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

                let drill1_level_key = rca_split[0].clone();
                let drill2_level_key = rca_split[1].clone();
                let measure = rca_split[2].clone();

                let level_name_1 = match level_map.get(&drill1_level_key) {
                    Some(l) => l,
                    None => bail!("Unable to find drill 1 level")
                };

                let level_name_2 = match level_map.get(&drill2_level_key) {
                    Some(l) => l,
                    None => bail!("Unable to find drill 2 level")
                };

                let rca = RcaQuery::new(
                    level_name_1.dimension.clone(),
                    level_name_1.hierarchy.clone(),
                    level_name_1.level.clone(),
                    level_name_2.dimension.clone(),
                    level_name_2.hierarchy.clone(),
                    level_name_2.level.clone(),
                    measure
                );

                Some(rca)
            },
            None => None
        };

        // TODO: Resolve named sets
        let rate = match agg_query_opt.rate {
            Some(rate) => {
                let level_value_split: Vec<String> = rate.split(".").map(|s| s.to_string()).collect();
                if level_value_split.len() != 2 {
                    bail!("Malformatted rate calculation specification.");
                }

                let level_name = match level_map.get(&level_value_split[0]) {
                    Some(level_name) => level_name.clone(),
                    None => bail!("Unrecognized level in rate calculation.")
                };
                let value = level_value_split[1].clone();

                let values: Vec<String> = value.split(",").map(|s| s.to_string()).collect();

                Some(RateQuery::new(level_name, values))
            },
            None => None
        };

        let debug = agg_query_opt.debug.unwrap_or(false);
        let sparse = agg_query_opt.sparse.unwrap_or(false);
        let exclude_default_members = agg_query_opt.exclude_default_members.unwrap_or(false);

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
            exclude_default_members,
            filters,
            rate,
            sparse,
        })
    }
}
