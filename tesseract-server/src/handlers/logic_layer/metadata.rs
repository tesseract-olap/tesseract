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

use crate::app::AppState;
use crate::logic_layer::{LogicLayerConfig};
use crate::util::boxed_error;

use tesseract_core::format::{format_records, FormatType};
use tesseract_core::names::LevelName;

use super::super::util;


/// Handles default members query when a format is not specified.
/// Default format is CSV.
pub fn logic_layer_members_default_handler(
    (req, _cube): (HttpRequest<AppState>, Path<()>)
) -> FutureResponse<HttpResponse>
{
    get_members(req, "jsonrecords".to_owned())
}


/// Handles members query when a format is specified.
pub fn logic_layer_members_handler(
    (req, cube_format): (HttpRequest<AppState>, Path<(String)>)
) -> FutureResponse<HttpResponse>
{
    get_members(req, cube_format.to_owned())
}


/// Performs members query.
pub fn get_members(
    req: HttpRequest<AppState>,
    format: String,
) -> FutureResponse<HttpResponse>
{
    let format = format.parse::<FormatType>();
    let format = match format {
        Ok(f) => f,
        Err(err) => return boxed_error(err.to_string()),
    };

    info!("Format: {:?}", format);

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

    let members_query = match QS_NON_STRICT.deserialize_str::<MembersQueryOpt>(query) {
        Ok(q) => q,
        Err(err) => return boxed_error(err.to_string())
    };

    let mut cube_name: String = members_query.cube.clone();
    let mut level_name: Option<LevelName> = None;

    match &logic_layer_config {
        Some(logic_layer_config) => {
            let mut level: String = members_query.level.clone();

            // Cube name may have been changed due to config substitutions
            cube_name = match &logic_layer_config.clone().substitute_cube_name(members_query.cube.clone()) {
                Ok(cube_name) => cube_name.clone(),
                Err(_) => return boxed_error("Unable to resolve this cube name.".to_string())
            };

            match &logic_layer_config.aliases {
                Some(aliases) => {
                    match &aliases.cubes {
                        Some(cubes) => {
                            for cube in cubes {

                                // TODO: Consider moving cube resolution here

                                if &cube.name == &cube_name {
                                    match &cube.levels {
                                        Some(levels) => {
                                            for level_prop_config in levels {
                                                if level_prop_config.unique_name == members_query.level {
                                                    let parsed_level_name_res: Result<LevelName, Error> = level_prop_config.current_name.clone().parse();
                                                    match parsed_level_name_res {
                                                        Ok(parsed_level_name) => level_name = Some(parsed_level_name),
                                                        Err(_) => ()
                                                    }

                                                    break;
                                                }
                                            }
                                        },
                                        None => ()
                                    }
                                }
                            }
                        },
                        None => ()
                    }
                },
                None => ()
            };
        },
        None => ()
    }

    // If level name is not yet set, try to set it from a cube object by a direct match
    let level_name = match level_name {
        Some(level_name) => Some(level_name),
        None => {
            let mut level_name: Option<LevelName> = None;

            for cube in &schema.cubes {
                if &cube.name == &cube_name {
                    for dimension in &cube.dimensions {
                        for hierarchy in &dimension.hierarchies {
                            for level in &hierarchy.levels {
                                if &level.name == &members_query.level {
                                    level_name = Some(LevelName {
                                        dimension: dimension.name.clone(),
                                        hierarchy: hierarchy.name.clone(),
                                        level: level.name.clone(),
                                    });
                                }
                            }
                        }
                    }
                }
            }

            level_name
        }
    };

    let level_name = match level_name {
        Some(level_name) => level_name,
        None => return boxed_error("Unable to find a level with the name provided".to_string())
    };

    println!(" ");
    println!("{:?}", cube_name);
    println!("{:?}", level_name);
    println!(" ");

    // TODO: Actually get the data
    //       - from regular tables
    //       - from inline tables

    // TODO: Add locale splitting

    let members_sql_and_headers = schema.members_sql(&cube_name, &level_name);

    println!(" ");
    println!("{:?}", members_sql_and_headers);
    println!(" ");

    let (members_sql, header) = match members_sql_and_headers {
        Ok(s) => s,
        Err(err) => {
            return Box::new(
                future::result(
                    Ok(HttpResponse::BadRequest().json(err.to_string()))
                )
            );
        },
    };

    req.state()
        .backend
        .exec_sql(members_sql)
        .from_err()
        .and_then(move |df| {
            let content_type = util::format_to_content_type(&format);

            match format_records(&header, df, format) {
                Ok(res) => Ok(HttpResponse::Ok().set(content_type).body(res)),
                Err(err) => Ok(HttpResponse::NotFound().json(err.to_string())),
            }
        })
        .responder()
}


#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MembersQueryOpt {
    pub cube: String,
    pub level: String,
    pub locale: Option<String>,
}
