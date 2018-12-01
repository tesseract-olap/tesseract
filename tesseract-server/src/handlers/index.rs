use actix_web::{
    HttpRequest,
    HttpResponse,
    Result as ActixResult,
};
use serde_derive::Serialize;

use crate::app::AppState;

pub fn index_handler(_req: HttpRequest<AppState>) -> ActixResult<HttpResponse> {
    Ok(HttpResponse::Ok().json(
        Status {
            status: "ok".to_owned(),
            // TODO set this as the Cargo.toml version, after structopt added
            version: "0.1.0".to_owned(),
        }
    ))
}

#[derive(Debug, Serialize)]
struct Status {
    status: String,
    version: String,
}
