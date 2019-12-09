use std::env;

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


#[derive(Debug, Deserialize, Serialize)]
pub struct SchemaDeleteOpt {
    pub secret: String,
    pub id: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SchemaUpdateOpt {
    pub secret: String,
    pub id: String,
    pub content: String,
}

pub fn schema_add_handler(req: HttpRequest<AppState>) -> FutureResponse<HttpResponse> {
    let query = req.query_string();

    lazy_static!{
        static ref QS_NON_STRICT: qs::Config = qs::Config::new(5, false);
    }

    let query = QS_NON_STRICT.deserialize_str::<SchemaUpdateOpt>(&query);
    let query = ok_or_404!(query);

    // For now use, flush secret as auth mechanism
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
    let backend = &req.state().backend;

    let tablepath = env::var("TESSERACT_DB_SCHEMA_TABLEPATH").expect("need tablepath");

    backend.add_schema(&tablepath, &query.id, &query.content)
        .and_then(move |_result| {
            Ok(HttpResponse::Ok().body("success".to_owned()))
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

pub fn schema_delete_handler(req: HttpRequest<AppState>) -> FutureResponse<HttpResponse> {
    let query = req.query_string();

    lazy_static!{
        static ref QS_NON_STRICT: qs::Config = qs::Config::new(5, false);
    }

    let query = QS_NON_STRICT.deserialize_str::<SchemaDeleteOpt>(&query);
    let query = ok_or_404!(query);

    // For now use, flush secret as auth mechanism
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
    let backend = &req.state().backend;

    let tablepath = env::var("TESSERACT_DB_SCHEMA_TABLEPATH").expect("need tablepath");

    backend.delete_schema(&tablepath, &query.id)
        .and_then(move |_result| {
            Ok(HttpResponse::Ok().body("success".to_owned()))
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


pub fn schema_update_handler(req: HttpRequest<AppState>) -> FutureResponse<HttpResponse> {
    let query = req.query_string();

    lazy_static!{
        static ref QS_NON_STRICT: qs::Config = qs::Config::new(5, false);
    }

    let query = QS_NON_STRICT.deserialize_str::<SchemaUpdateOpt>(&query);
    let query = ok_or_404!(query);

    // For now use, flush secret as auth mechanism
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
    let backend = &req.state().backend;

    let tablepath = env::var("TESSERACT_DB_SCHEMA_TABLEPATH").expect("need tablepath");

    backend.update_schema(&tablepath, &query.id, &query.content)
        .and_then(move |_result| {
            Ok(HttpResponse::Ok().body("success".to_owned()))
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
