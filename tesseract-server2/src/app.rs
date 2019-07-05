use actix_web::{
    http::Method,
    middleware,
    App,
    dev::Body,
    web,
    middleware::NormalizePath,
};
use actix_service::{NewService, Service};

use tesseract_core::{Backend, Schema};

use crate::db_config::Database;

use crate::handlers;
// use crate::logic_layer::{Cache, LogicLayerConfig};

use std::sync::{Arc, RwLock};


/// Holds data about the source of a schema file.
#[derive(Debug, Clone)]
pub enum SchemaSource {
    LocalSchema { filepath: String },
    #[allow(dead_code)]
    RemoteSchema { endpoint: String },
}

/// Holds a struct of environment variables that will be accessed through the `AppState`.
#[derive(Debug, Clone)]
pub struct EnvVars {
    pub database_url: String,
    pub schema_source: SchemaSource,
    pub flush_secret: Option<String>,
}

/// Holds [ActixWeb State](https://actix.rs/docs/application/).
pub struct AppState {
    pub debug: bool,
    pub backend: Box<dyn Backend + Sync + Send>,
    // TODO this is a hack, until a better interface is set up with the Backend Trait
    // to generate its own sql.
    pub db_type: Database,
    pub env_vars: EnvVars,
    pub schema: Arc<RwLock<Schema>>,
    // pub cache: Arc<RwLock<Cache>>,
    // pub logic_layer_config: Option<Arc<RwLock<LogicLayerConfig>>>,
}

pub fn streaming_agg_config(cfg: &mut web::ServiceConfig) {
    cfg
        .route("/cubes/{cube}/aggregate", web::get().to_async(handlers::aggregate_stream_default_handler))
        .route("/cubes/{cube}/aggregate.{format}", web::get().to_async(handlers::aggregate_stream_handler));
}

pub fn standard_agg_config(cfg: &mut web::ServiceConfig) {
    cfg
        .route("/cubes/{cube}/aggregate", web::get().to_async(handlers::aggregate_default_handler))
        .route("/cubes/{cube}/aggregate.{format}", web::get().to_async(handlers::aggregate_handler));
}


pub fn base_config(cfg: &mut web::ServiceConfig) {
    cfg
        .route("/", web::get().to(handlers::index_handler))
        .route("/cubes", web::get().to(handlers::metadata_all_handler))
        .route("/cubes/{cubes}", web::get().to(handlers::metadata_handler))
        .route("/cubes/{cube}/members", web::get().to_async(handlers::members_default_handler))
        .route("/cubes/{cube}/members.{format}", web::get().to_async(handlers::members_handler))
        .route("/flush", web::get().to(handlers::flush_handler));
}
