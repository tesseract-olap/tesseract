use std::collections::HashMap;
use actix_web::{
    FutureResponse,
    HttpRequest,
    HttpResponse,
};
use futures::future::{self};
use actix_web::http::header::ContentType;
use mime;

use tesseract_core::format::FormatType;
use tesseract_core::schema::Cube;
use tesseract_core::schema::metadata::SourceMetadata;

use crate::app::AppState;

use failure::{bail, format_err, Error};
use tesseract_core::names::Cut;
use crate::logic_layer::CubeCache;
use crate::auth::{validate_web_token, extract_token, user_auth_level};

pub(crate) fn format_to_content_type(format_type: &FormatType) -> ContentType {
    match format_type {
        FormatType::Csv => ContentType(mime::TEXT_CSV_UTF_8),
        FormatType::JsonRecords => ContentType(mime::APPLICATION_JSON),
        FormatType::JsonArrays => ContentType(mime::APPLICATION_JSON),
    }
}


/// Helper method to return errors (FutureResponse<HttpResponse>) from String.
pub fn boxed_error_string(message: String) -> FutureResponse<HttpResponse> {
    Box::new(
        future::result(
            Ok(HttpResponse::NotFound().json(message))
        )
    )
}

/// Helper method to return errors (FutureResponse<HttpResponse>) from HttpResponse.
pub fn boxed_error_http_response(response: HttpResponse) -> FutureResponse<HttpResponse> {
    Box::new(future::result(Ok(response)))
}

// Genrates the source data/ annotaion of the cube for which the query is executed
pub fn generate_source_data(cube: &Cube) -> SourceMetadata {
    let cube_name = &cube.name;
    let mut measures = Vec::new();
    for measure in cube.measures.iter() {
        measures.push(measure.name.clone());
    }
    let annotations = match cube.annotations.clone(){
        Some(annotations) => {
            let mut anotate_hashmap = HashMap::new();
            for annotation in annotations.iter(){
                anotate_hashmap.insert(annotation.name.to_string(), annotation.text.to_string());
            }
            Some(anotate_hashmap)
        },
        None => None
    };
    SourceMetadata {
        name: cube_name.clone(),
        measures: measures.clone(),
        annotations: annotations.clone(),
    }
}

pub fn get_user_auth_level(req: &HttpRequest<AppState>) -> Option<i32> {
    let jwt_secret = &req.state().env_vars.jwt_secret;
    let user_token = extract_token(req);
    user_auth_level(jwt_secret, &user_token)
}

pub fn verify_authorization(req: &HttpRequest<AppState>, min_auth_level: i32) -> Result<(), HttpResponse> {
    let jwt_secret = &req.state().env_vars.jwt_secret;
    let user_token = extract_token(req);
    if !validate_web_token(jwt_secret, &user_token, min_auth_level) {
        return Err(HttpResponse::Unauthorized().json("This cube is not public".to_string()));
    }

    Ok(())
}


#[macro_export]
macro_rules! ok_or_400 {
    ($expr:expr) => {
        match $expr {
            Ok(val) => val,
            Err(err) => {
                return Box::new(
                    future::result(
                        Ok(HttpResponse::BadRequest().json(err.to_string()))
                    )
                );
            }
        }
    };
}


#[macro_export]
macro_rules! ok_or_404 {
    ($expr:expr) => {
        match $expr {
            Ok(val) => val,
            Err(err) => {
                return Box::new(
                    future::result(
                        Ok(HttpResponse::NotFound().json(err.to_string()))
                    )
                );
            }
        }
    };
}


#[macro_export]
macro_rules! some_or_404 {
    ($expr:expr, $note:expr) => {
        match $expr {
            Some(val) => val,
            None => {
                return Box::new(
                    future::result(
                        Ok(HttpResponse::NotFound().json($note.to_string()))
                    )
                );
            }
        }
    };
}


pub fn validate_members(cuts: &[Cut], cube_cache: &CubeCache) -> Result<(), Error> {
    for cut in cuts {
        // get level cache
        let member_cache = cube_cache.members_for_level(&cut.level_name)
            .ok_or_else(|| format_err!("Level not found in cache"))?;
        for member in &cut.members {
            if !member_cache.contains(member) {
                bail!("Cut member not found");
            }
        }
    }
    Ok(())
}