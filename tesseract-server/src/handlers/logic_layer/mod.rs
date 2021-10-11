pub mod aggregate;
pub mod geoservice;
pub mod metadata;
pub mod relations;

pub use self::geoservice::GeoserviceQuery;
pub use self::geoservice::GeoServiceResponseJson;
pub use self::geoservice::query_geoservice;

use actix_web::{web, HttpRequest, HttpResponse, ResponseError};
use crate::app::AppState;
use crate::errors::ServerError;
use tesseract_core::CubeHasUniqueLevelsAndProperties;


pub async fn non_unique_levels_default_handler(
    req: HttpRequest,
    state: web::Data<AppState>,
    cube_format: web::Path<()>,
    ) -> HttpResponse
{
    if state.debug {
        // must be true, but have to de-structure again after doing it before in app.rs;
        if let CubeHasUniqueLevelsAndProperties::False { cube, name } = &state.has_unique_levels_properties {
            ServerError::LogicLayerDuplicateNames { cube: cube.clone(), name: name.clone() }.error_response()
        } else {
            unreachable!();
        }
    } else {
        ServerError::ErrorCode { code: "555".to_owned() }.error_response()
    }
}


pub async fn non_unique_levels_handler(
    req: HttpRequest,
    state: web::Data<AppState>,
    cube_format: web::Path<String>,
    ) -> HttpResponse
{
    if state.debug {
        // must be true, but have to de-structure again after doing it before in app.rs;
        if let CubeHasUniqueLevelsAndProperties::False { cube, name } = &state.has_unique_levels_properties {
            ServerError::LogicLayerDuplicateNames { cube: cube.clone(), name: name.clone() }.error_response()
        } else {
            unreachable!();
        }
    } else {
        ServerError::ErrorCode { code: "555".to_owned() }.error_response()
    }
}


pub async fn relations_non_unique_levels_default_handler(
    req: HttpRequest,
    state: web::Data<AppState>,
    cube_format: web::Path<()>,
    ) -> HttpResponse
{
    if state.debug {
        // must be true, but have to de-structure again after doing it before in app.rs;
        if let CubeHasUniqueLevelsAndProperties::False { cube, name } = &state.has_unique_levels_properties {
            ServerError::LogicLayerDuplicateNames { cube: cube.clone(), name: name.clone() }.error_response()
        } else {
            unreachable!();
        }
    } else {
        ServerError::ErrorCode { code: "555".to_owned() }.error_response()
    }
}


pub async fn relations_non_unique_levels_handler(
    req: HttpRequest,
    state: web::Data<AppState>,
    cube_format: web::Path<String>,
    ) -> HttpResponse
{
    if state.debug {
        // must be true, but have to de-structure again after doing it before in app.rs;
        if let CubeHasUniqueLevelsAndProperties::False { cube, name } = &state.has_unique_levels_properties {
            ServerError::LogicLayerDuplicateNames { cube: cube.clone(), name: name.clone() }.error_response()
        } else {
            unreachable!();
        }
    } else {
        ServerError::ErrorCode { code: "555".to_owned() }.error_response()
    }
}
