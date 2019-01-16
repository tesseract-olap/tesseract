use actix_web::{
    http::Method,
    middleware,
    App,
};
use tesseract_core::{Backend, Schema};

use crate::db_config::Database;
use crate::handlers::{
    aggregate_handler,
    aggregate_default_handler,
    flush_handler,
    index_handler,
    metadata_handler,
    metadata_all_handler,
};

use std::sync::{Arc, RwLock};

pub struct LocalSchema {
    filepath: String,
}

pub struct RemoteSchema {
    endpoint: String,
}

#[derive(Debug, Clone)]
pub enum SchemaSource {
    LocalSchema {filepath: String},
    RemoteSchema {endpoint: String},
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
    pub backend: Box<dyn Backend + Sync + Send>,
    // TODO this is a hack, until a better interface is set up with the Backend Trait
    // to generate its own sql.
    pub db_type: Database,
    pub schema: Arc<RwLock<Schema>>,
    pub env_vars: EnvVars,
}

/// Creates an ActixWeb application with an `AppState`.
pub fn create_app(backend: Box<dyn Backend + Sync + Send>, db_type: Database, schema: Arc<RwLock<Schema>>, env_vars: EnvVars) -> App<AppState> {
    App::with_state(AppState { backend, db_type, schema, env_vars })
        .middleware(middleware::Logger::default())
        .resource("/", |r| {
            r.method(Method::GET).with(index_handler)
        })
        .resource("/cubes", |r| {
            r.method(Method::GET).with(metadata_all_handler)
        })
        .resource("/cubes/{cube}", |r| {
            r.method(Method::GET).with(metadata_handler)
        })
        .resource("/cubes/{cube}/aggregate", |r| {
            r.method(Method::GET).with(aggregate_default_handler)
        })
        .resource("/cubes/{cube}/aggregate.{format}", |r| {
            r.method(Method::GET).with(aggregate_handler)
        })
        .resource("/flush", |r| {
            // TODO: Change this to POST?
            r.method(Method::GET).with(flush_handler)
        })
}
