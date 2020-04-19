use serde_derive::{Serialize, Deserialize};

use lazy_static::lazy_static;
use log::*;
use serde_qs as qs;
use futures::future::{self, Future};
use crate::errors::ServerError;

use actix_web::{
    AsyncResponder,
    HttpRequest,
    HttpResponse,
    FutureResponse,
};

use crate::app::{AppState};
use crate::schema_config;


#[derive(Debug, Deserialize, Serialize)]
pub struct FlushQueryOpt {
    pub secret: String,
}

pub fn flush_handler(req: HttpRequest<AppState>) -> FutureResponse<HttpResponse> {
    let query = req.query_string();

    lazy_static!{
        static ref QS_NON_STRICT: qs::Config = qs::Config::new(5, false);
    }

    let query_res = QS_NON_STRICT.deserialize_str::<FlushQueryOpt>(&query);
    let query = ok_or_404!(query_res);

    match &req.state().env_vars.flush_secret {
        Some(db_secret) => {
            if *db_secret != query.secret {
                auth_denied!("Authorization denied, bad secret.".to_string());
            }
        },
        None => { auth_denied!("Authorization denied. No secret set.".to_string()) }
    }

    info!("Flush internal state");
        let debug_mode = req.state().debug;
        // Read schema again
        // NOTE: This logic will change once we start supporting remote schemas
        let schema_config = &req.state().env_vars.schema_source;
        schema_config::reload_schema(schema_config, req.state().backend.clone())
            .and_then(move |vec_strs| {
                let content = schema_config::merge_schemas(&vec_strs);
                // convert final string to schema Object
                let schema = schema_config::read_schema(&content, &"json".to_string()).expect("schema ok");

                // Update shared schema
                let mut w = req.state().schema.write().unwrap();
                *w = schema.clone();
                Ok(HttpResponse::Ok()
                        .body("success".to_owned()))
            })
            .map_err(move |e| {
                if debug_mode {
                    ServerError::Db { cause: e.to_string() }.into()
                } else {
                    ServerError::Db { cause: "Internal Server Error 4040".to_owned() }.into()
                }
            })
            .responder()
}
