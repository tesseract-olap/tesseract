use serde_derive::{Serialize, Deserialize};

use lazy_static::lazy_static;
use log::*;
use serde_qs as qs;

use actix_web::{
    HttpRequest,
    HttpResponse,
    Result as ActixResult,
};

use crate::app::{AppState, SchemaSource};
use crate::schema_config;

use crate::logic_layer::{populate_cache, LogicLayerConfig};


#[derive(Debug, Deserialize, Serialize)]
pub struct CacheRefreshQueryOpt {
    pub secret: String,
    pub cube: String,
}

pub fn cache_refresh_handler(req: HttpRequest<AppState>) -> ActixResult<HttpResponse> {
    let query = req.query_string();

    lazy_static!{
        static ref QS_NON_STRICT: qs::Config = qs::Config::new(5, false);
    }

    let query_res = QS_NON_STRICT.deserialize_str::<CacheRefreshQueryOpt>(&query);
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
        info!("Refresh cache...");

        // According to https://stackoverflow.com/questions/54447087/rust-actix-get-systemrunner-for-systemcurrent,
        // you can't block on an running System, so we need to create a
        // new one temporarily to run the cache refresh.
        let mut sys = actix::System::new("tesseract-cache-refresh");

        info!("Got sys...");

        let schema = req.state().schema.read().unwrap();
        let backend = req.state().backend.clone();
        let logic_layer_config: Option<LogicLayerConfig> = match &req.state().logic_layer_config {
            Some(llc) => Some(llc.read().unwrap().clone()),
            None => None
        };

        let mut cache = req.state().cache.write().unwrap();

        for cube in &schema.cubes {
            if cube.name == query.cube {
                cache.refresh_cube_cache(
                    cube,
                    &logic_layer_config,
                    backend,
                    &mut sys
                );

                break;
            }
        }

        Ok(HttpResponse::Ok().finish())
    } else {
        Ok(HttpResponse::Unauthorized().finish())
    }
}
