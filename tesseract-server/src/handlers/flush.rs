use serde_derive::{Serialize, Deserialize};

use lazy_static::lazy_static;
use log::*;
use serde_qs as qs;

use actix_web::{
    HttpRequest,
    HttpResponse,
    Result as ActixResult,
};

use crate::app;
use crate::schema_config;

use app::{AppState, SchemaSource, LocalSchema, RemoteSchema};

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

    let db_secret = match &req.state().env_vars.flush_secret {
        Some(db_secret) => db_secret,
        None => { return Ok(HttpResponse::Unauthorized().finish()); }
    };

    if query.secret == *db_secret {
        info!("Flush internal state");

        // Read schema again
        // NOTE: This logic will change once we start supporting remote schemas
        let schema_path = match &req.state().env_vars.schema_source {
            SchemaSource::LocalSchema { ref filepath } => filepath,
            SchemaSource::RemoteSchema { ref endpoint } => endpoint,
        };
        let schema = match schema_config::read_schema(&schema_path) {
            Ok(val) => val,
            Err(err) => {
                error!("{}", err);
                return Ok(HttpResponse::InternalServerError().finish());
            },
        };

        // Update shared schema
        let mut w = req.state().schema.write().unwrap();
        *w = schema;

        // TODO: Clear internal cache once that's implemented

        Ok(HttpResponse::Ok().finish())
    } else {
        Ok(HttpResponse::Unauthorized().finish())
    }
}
