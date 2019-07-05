mod aggregate;
pub mod shared;

pub use self::aggregate::logic_layer_handler;
pub use self::aggregate::logic_layer_default_handler;
pub use self::shared::{Time, TimePrecision, TimeValue, LogicLayerQueryOpt};

use actix_web::{HttpRequest, HttpResponse, web::Path};

pub fn logic_layer_non_unique_levels_default_handler(_req: HttpRequest, _cube: Path<()>) -> HttpResponse
{
    HttpResponse::InternalServerError().body("Error Code 555")
}

pub fn logic_layer_non_unique_levels_handler(_req: HttpRequest, _cube: Path<(String)>) -> HttpResponse
{
    HttpResponse::InternalServerError().body("Error Code 555")
}
