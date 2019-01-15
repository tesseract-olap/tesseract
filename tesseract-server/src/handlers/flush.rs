use serde_derive::{Serialize, Deserialize};

use lazy_static::lazy_static;
use log::*;
use serde_qs as qs;
use std::env;

use actix_web::{
    HttpRequest,
    HttpResponse,
    Result as ActixResult,
};

use crate::app::AppState;
use crate::schema_config;

#[derive(Debug, Deserialize, Serialize)]
pub struct FlushQueryOpt {
    pub secret: String,
}

pub fn flush_handler(req: HttpRequest<AppState>) -> ActixResult<HttpResponse> {
    let query = req.query_string();
    lazy_static!{
        static ref QS_NON_STRICT: qs::Config = qs::Config::new(5, false);
    }
    let query_res = QS_NON_STRICT.deserialize_str::<FlushQueryOpt>(&query);
    let query = match query_res {
        Ok(q) => q,
        Err(err) => {
            return Ok(HttpResponse::BadRequest().json(err.to_string()));
        },
    };

    let db_secret = match env::var("TESSERACT_FLUSH_SECRET") {
        Ok(val) => val,
        Err(err) => {
            error!("{}", err);
            return Ok(HttpResponse::InternalServerError().finish());
        },
    };

    if query.secret == db_secret {
        info!("Flush internal state");

        // TODO: Re-read and set schema (watch for concorrency issues)
        let schema = match schema_config::read_schema() {
            Ok(val) => val,
            Err(err) => {
                error!("{}", err);
                return Ok(HttpResponse::InternalServerError().finish());
            },
        };

        info!("{:?}", req.state().schema);

        // TODO: Clear internal cache once that's implemented

        Ok(HttpResponse::Ok().finish())
    } else {
        Ok(HttpResponse::Unauthorized().finish())
    }
}
