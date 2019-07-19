mod aggregate;
pub mod shared;

pub use self::aggregate::logic_layer_handler;
pub use self::aggregate::logic_layer_default_handler;
pub use self::shared::{Time, TimePrecision, TimeValue, boxed_error};

use actix_web::{HttpRequest, HttpResponse, Path};
use crate::app::AppState;

pub fn logic_layer_non_unique_levels_default_handler(
    (_req, _cube): (HttpRequest<AppState>, Path<()>),
    ) -> HttpResponse
{
    HttpResponse::InternalServerError().body("Error Code 555")
}

pub fn logic_layer_non_unique_levels_handler(
    (_req, _cube): (HttpRequest<AppState>, Path<(String)>),
    ) -> HttpResponse
{
    HttpResponse::InternalServerError().body("Error Code 555")
}
