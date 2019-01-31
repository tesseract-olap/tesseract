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
    ll_aggregate_handler,
    ll_aggregate_default_handler,
    cube_detection_aggregation_default_handler,
    cube_detection_aggregation_handler,
    flush_handler,
    index_handler,
    metadata_handler,
    metadata_all_handler,
};
use crate::logic_layer::{Cache, populate_cache};

use std::sync::{Arc, RwLock};


/// Holds data about the source of a schema file.
#[derive(Debug, Clone)]
pub enum SchemaSource {
    LocalSchema { filepath: String },
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
    pub backend: Box<dyn Backend + Sync + Send>,
    // TODO this is a hack, until a better interface is set up with the Backend Trait
    // to generate its own sql.
    pub db_type: Database,
    pub env_vars: EnvVars,
    pub schema: Arc<RwLock<Schema>>,
    pub cache: Arc<RwLock<Cache>>,
}

/// Creates an ActixWeb application with an `AppState`.
pub fn create_app(backend: Box<dyn Backend + Sync + Send>, db_type: Database, env_vars: EnvVars, schema: Arc<RwLock<Schema>>, cache: Arc<RwLock<Cache>>) -> App<AppState> {
    App::with_state(AppState { backend, db_type, env_vars, schema, cache })
        .middleware(middleware::Logger::default())
        // Metadata
        .resource("/", |r| {
            r.method(Method::GET).with(index_handler)
        })
        .resource("/cubes", |r| {
            r.method(Method::GET).with(metadata_all_handler)
        })
        .resource("/cubes/{cube}", |r| {
            r.method(Method::GET).with(metadata_handler)
        })

        // Aggregation
        .resource("/cubes/{cube}/aggregate", |r| {
            r.method(Method::GET).with(aggregate_default_handler)
        })
        .resource("/cubes/{cube}/aggregate.{format}", |r| {
            r.method(Method::GET).with(aggregate_handler)
        })

        // Aggregation + Logic Layer
        // TODO: Consolidate these routes into the aggregate routes above
        .resource("/cubes/{cube}/logic-layer", |r| {
            r.method(Method::GET).with(ll_aggregate_default_handler)
        })
        .resource("/cubes/{cube}/logic-layer.{format}", |r| {
            r.method(Method::GET).with(ll_aggregate_handler)
        })
        .resource("/aggregate", |r| {
            r.method(Method::GET).with(cube_detection_aggregation_default_handler)
        })
        .resource("/aggregate.{format}", |r| {
            r.method(Method::GET).with(cube_detection_aggregation_handler)
        })

        // Helpers
        .resource("/flush", |r| {
            r.method(Method::POST).with(flush_handler)
        })
}
