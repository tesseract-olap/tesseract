use std::collections::HashMap;

use actix_web::{
    AsyncResponder,
    FutureResponse,
    HttpRequest,
    HttpResponse,
    Path,
};
use failure::{Error, format_err, bail};
use futures::future::{Future};
use lazy_static::lazy_static;
use log::*;
use serde_qs as qs;
use serde_derive::Deserialize;

use tesseract_core::names::{Cut, Drilldown, Property, Measure};
use tesseract_core::format::{format_records, FormatType};
use tesseract_core::query::{FilterQuery, GrowthQuery, RcaQuery, TopQuery, RateQuery};
use tesseract_core::{Query as TsQuery, Schema, MeaOrCalc};
use tesseract_core::schema::{Cube};

use crate::app::AppState;
use crate::handlers::logic_layer::shared::{Time, boxed_error};
use crate::errors::ServerError;
use crate::logic_layer::{LogicLayerConfig, CubeCache};
use super::super::util;


/// Handles default aggregation when a format is not specified.
/// Default format is CSV.
pub fn logic_layer_default_handler(
    (req, _cube): (HttpRequest<AppState>, Path<()>)
) -> FutureResponse<HttpResponse>
{
    logic_layer_aggregation(req, "jsonrecords".to_owned())
}

/// Handles aggregation when a format is specified.
pub fn logic_layer_handler(
    (req, cube_format): (HttpRequest<AppState>, Path<(String)>)
) -> FutureResponse<HttpResponse>
{
    logic_layer_aggregation(req, cube_format.to_owned())
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

/// Performs data aggregation.
pub fn logic_layer_aggregation(
    req: HttpRequest<AppState>,
    format: String,
) -> FutureResponse<HttpResponse>
{
    let format = format.parse::<FormatType>();
    let format = match format {
        Ok(f) => f,
        Err(err) => return boxed_error(err.to_string()),
    };

    info!("format: {:?}", format);

    let query = req.query_string();
    let schema = req.state().schema.read().unwrap();
    let debug = req.state().debug;

    let logic_layer_config: Option<LogicLayerConfig> = match &req.state().logic_layer_config {
        Some(llc) => Some(llc.read().unwrap().clone()),
        None => None
    };

    lazy_static!{
        static ref QS_NON_STRICT: qs::Config = qs::Config::new(5, false);
    }

    let mut cube_name;

    let mut agg_query = match QS_NON_STRICT.deserialize_str::<LogicLayerQueryOpt>(query) {
        Ok(mut q) => {
            // Check to see if the logic layer config has a alias with the
            // provided cube name
            cube_name = match logic_layer_config.clone() {
                Some(llc) => {
                    match llc.sub_cube_name(q.cube.clone()) {
                        Ok(cn) => cn,
                        Err(_) => q.cube.clone()
                    }
                },
                None => q.cube.clone()
            };

            let cube = match schema.get_cube_by_name(&cube_name) {
                Ok(c) => c.clone(),
                Err(err) => return boxed_error(err.to_string())
            };

            let cube_cache = req.state().cache.read().unwrap().find_cube_info(&cube_name).clone();

            // Hack for now since can't provide extra arguments on try_into
            q.cube_obj = Some(cube.clone());
            q.cube_cache = cube_cache;
            q.config = logic_layer_config;
            q.schema = Some(schema.clone());
            q
        },
        Err(err) => return boxed_error(err.to_string())
    };

    // Process `time` param (latest/oldest)
    match &agg_query.time {
        Some(s) => {
            let cube_cache = req.state().cache.read().unwrap().find_cube_info(&cube_name);

            let time_cuts: Vec<String> = s.split(",").map(|s| s.to_string()).collect();

            for time_cut in time_cuts {
                let tc: Vec<String> = time_cut.split(".").map(|s| s.to_string()).collect();

                if tc.len() != 2 {
                    return boxed_error("Malformatted time cut".to_string());
                }

                let time = match Time::from_key_value(tc[0].clone(), tc[1].clone()) {
                    Ok(time) => time,
                    Err(err) => return boxed_error(err.to_string())
                };

                match cube_cache.clone() {
                    Some(cache) => {
                        let (cut, cut_value) = match cache.get_time_cut(time) {
                            Ok(cut) => cut,
                            Err(err) => return boxed_error(err.to_string())
                        };

                        agg_query.cuts = match agg_query.cuts {
                            Some(mut cuts) => {
                                cuts.insert(cut, cut_value);
                                Some(cuts)
                            },
                            None => {
                                let mut m: HashMap<String, String> = HashMap::new();
                                m.insert(cut, cut_value);
                                Some(m)
                            },
                        }
                    },
                    None => (),
                };
            }
        },
        None => (),
    }

    info!("Aggregate query: {:?}", agg_query);

    // TODO: Create TsQuery from here

    // Turn AggregateQueryOpt into TsQuery
    let ts_query: Result<Vec<TsQuery>, _> = generate_ts_queries(agg_query.clone());
    let ts_query = match ts_query {
        Ok(q) => q[0].clone(),
        Err(err) => return boxed_error(err.to_string())
    };

    info!("Tesseract query: {:?}", ts_query);

    let query_ir_headers = req
        .state()
        .schema.read().unwrap()
        .sql_query(&cube_name, &ts_query);
    let (query_ir, headers) = match query_ir_headers {
        Ok(x) => x,
        Err(err) => return boxed_error(err.to_string())
    };

    info!("Query IR: {:?}", query_ir);

    let sql = req.state()
        .backend
        .generate_sql(query_ir);

    info!("SQL query: {}", sql);
    info!("Headers: {:?}", headers);

    req.state()
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
            if debug {
                ServerError::Db { cause: e.to_string() }.into()
            } else {
                ServerError::Db { cause: "Internal Server Error 1010".to_owned() }.into()
            }
        })
        .responder()
}

pub fn generate_ts_queries(agg_query_opt: LogicLayerQueryOpt) -> Result<Vec<TsQuery>, Error> {

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

                    // TODO: Add support for double query when children and self are requested

                    // Identify child level
                    let child_level =  match cube.get_child_level(level_name)? {
                        Some(cl) => cl,
                        None => bail!("Could not find child level for `{}`", level_name.level)
                    };

                    // Add a child level drilldown
                    let drilldown = Drilldown::new(
                        level_name.dimension.clone(),
                        level_name.hierarchy.clone(),
                        child_level.name.clone()
                    );

                    drilldowns.push(drilldown);

                    // Add captions for this level
                    let new_captions = level.get_captions(&level_name, &locales);
                    captions.extend_from_slice(&new_captions);

                    // Add a cut on this level
                    let (mask, for_match, _cut_val) = Cut::parse_cut(&cut_val);

                    let cut = Cut::new(
                        level_name.dimension.clone(),
                        level_name.hierarchy.clone(),
                        level_name.level.clone(),
                        final_members, mask, for_match
                    );

                    cuts.push(cut);

                } else if operation == "parent".to_string() {

                    // Country=1,parent
                    // - [ ] Identify Continent as the parent level
                    // - [ ] Somehow find EXACTLY which continent is the parent of this country
                    // - [ ] Add a cut for that Continent:
                    //   - e.g. Continent=1
                    // - [ ] Perform two queries and join the result

                    // This is done all the way up so it will require much more than 2 queries

                    return Err(format_err!("`parent` operation not currently supported."));

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
                let (mask, for_match, _cut_val) = Cut::parse_cut(&cut_val);

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

    Ok(vec![TsQuery {
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
    }])

}
