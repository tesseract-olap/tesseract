use actix_web::{
    AsyncResponder,
    FutureResponse,
    HttpRequest,
    HttpResponse,
    Path,
    Result as ActixResult
};

use futures::future::{self, Future};
use lazy_static::lazy_static;
use log::*;
use serde_derive::Deserialize;
use serde_qs as qs;
use tesseract_core::format::{format_records, FormatType};
use tesseract_core::names::{LevelName, Property};
use tesseract_core::schema::metadata::{CubeMetadata, PropertyMetadata};
use tesseract_core::DEFAULT_ALLOWED_ACCESS;

use crate::app::AppState;
use crate::logic_layer::LogicLayerConfig;
use super::util::{boxed_error_http_response, verify_authorization, get_user_auth_level};


pub fn metadata_handler(
    (req, cube): (HttpRequest<AppState>, Path<String>)
    ) -> ActixResult<HttpResponse>
{
    info!("Metadata for cube: {}", cube);
    let cube = match req.state().schema.read().unwrap().cube_metadata(&cube){
        Some(c) => c,
        None => return Ok(HttpResponse::NotFound().finish()),
    };

    if let Err(err) = verify_authorization(&req, cube.min_auth_level) {
        return Ok(err);
    }

    let ll_config = match &req.state().logic_layer_config {
        Some(llc) => llc.read().unwrap().clone(),
        None => return  Ok(HttpResponse::Ok().json(cube))
    };
    let cube_details = get_cube_metadata(cube, &ll_config);
    Ok(HttpResponse::Ok().json(cube_details))
}


pub fn metadata_all_handler(
    req: HttpRequest<AppState>
    ) -> ActixResult<HttpResponse>
{
    info!("Metadata for all");
    let user_auth_level = get_user_auth_level(&req);
    let mut schema_details = req.state().schema.read().unwrap().metadata(user_auth_level);
    let ll_config = match &req.state().logic_layer_config {
        Some(llc) => llc.read().unwrap().clone(),
        None => {
            return  Ok(HttpResponse::Ok().json(schema_details))
        }
    };
    let mut cubes: Vec<CubeMetadata> = Vec::new();
    for cube in schema_details.cubes.iter(){
        // Filter out cube that user isn't authorized to see
        match user_auth_level {
            Some(auth_level) => { // Authorization is set
                if auth_level >= cube.min_auth_level && auth_level >= DEFAULT_ALLOWED_ACCESS {
                    cubes.push(get_cube_metadata(cube.clone(), &ll_config));
                }
            },
            // No authorization set. Show all cubes
            None => cubes.push(get_cube_metadata(cube.clone(), &ll_config))
        }
    }
    schema_details.cubes = cubes;
    Ok(HttpResponse::Ok().json(schema_details))
}


pub fn members_default_handler(
    (req, cube): (HttpRequest<AppState>, Path<String>)
    ) -> FutureResponse<HttpResponse>
{
    let cube_format = (cube.into_inner(), "csv".to_owned());
    do_members(req, cube_format)
}


pub fn members_handler(
    (req, cube_format): (HttpRequest<AppState>, Path<(String, String)>)
    ) -> FutureResponse<HttpResponse>
{
    do_members(req, cube_format.into_inner())
}


pub fn get_cube_metadata(
    mut cube_details: CubeMetadata,
    ll_config: &LogicLayerConfig,
) -> CubeMetadata {
    cube_details.alias = ll_config.find_cube_aliases(&cube_details.name);
    for dimension in cube_details.dimensions.iter_mut(){
        for hierarchy in dimension.hierarchies.iter_mut(){
            for level in hierarchy.levels.iter_mut(){
                let cube_name = &cube_details.name;
                let dimension_name = &dimension.name;
                let level_name = LevelName::new(
                    &dimension.name,
                    &hierarchy.name,
                    &level.name,
                );
                let unique =match ll_config
                            .find_unique_cube_level_name(cube_name, &level_name)
                            .or_else(|_| {ll_config.find_unique_shared_dimension_level_name(dimension_name, cube_name, &level_name)}){
                                Ok(u) => u,
                                Err(_) => None
                            };
                level.unique_name = unique;
                let mut properties_list: Vec<PropertyMetadata> = Vec::new();
                match &level.properties {
                    Some(p) => {
                        for property in p.iter(){
                            let property_name = Property::new(
                                &dimension.name,
                                &hierarchy.name,
                                &level.name,
                                &property.name,
                            );
                            let unique_property_name = match ll_config
                                        .find_unique_cube_property_name(cube_name, &property_name)
                                        .or_else(|_| {ll_config.find_unique_shared_dimension_property_name(dimension_name, cube_name, &property_name)}){
                                            Ok(u) => u,
                                            Err(_) => None
                                        };
                            properties_list.push(PropertyMetadata{
                                name: property.name.clone(),
                                caption_set: property.caption_set.clone(),
                                annotations: property.annotations.clone(),
                                unique_name: unique_property_name,
                            });
                        };
                        level.properties=Some(properties_list.clone())
                    },
                    None => level.properties = None
                }
            }
        }
    }
    cube_details
}


pub fn do_members(
    req: HttpRequest<AppState>,
    cube_format: (String, String),
    ) -> FutureResponse<HttpResponse>
{
    let (cube, format) = cube_format;

    // Get cube object to check for API key
    let schema = &req.state().schema.read().unwrap().clone();
    let cube_obj = ok_or_404!(schema.get_cube_by_name(&cube));

    if let Err(err) = verify_authorization(&req, cube_obj.min_auth_level) {
        return boxed_error_http_response(err);
    }

    let format = ok_or_404!(format.parse::<FormatType>());

    let query = req.query_string();

    lazy_static!{
        static ref QS_NON_STRICT: qs::Config = qs::Config::new(5, false);
    }

    let query_res = QS_NON_STRICT.deserialize_str::<MembersQueryOpt>(&query);
    let query = ok_or_400!(query_res);

    let level: LevelName = ok_or_400!(query.level.parse());

    info!("Members for cube: {}, level: {}", cube, level);

    let members_sql_and_headers = req.state().schema.read().unwrap()
        .members_sql(&cube, &level);

    let (members_sql, header) = ok_or_400!(members_sql_and_headers);

    req.state()
        .backend
        .exec_sql(members_sql)
        .from_err()
        .and_then(move |df| {
            match format_records(&header, df, format, None, false) {
                Ok(res) => Ok(HttpResponse::Ok().body(res)),
                Err(err) => Ok(HttpResponse::NotFound().json(err.to_string())),
            }
        })
        .responder()
}


#[derive(Debug, Deserialize)]
struct MembersQueryOpt {
    level: String,
}
