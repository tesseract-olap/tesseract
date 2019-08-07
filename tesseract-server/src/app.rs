use actix_web::{
    http::Method,
    middleware,
    App,
    http::NormalizePath,
};
use tesseract_core::{Backend, Schema};

use crate::db_config::Database;
use crate::handlers::{
    aggregate_handler,
    aggregate_default_handler,
    aggregate_stream_handler,
    aggregate_stream_default_handler,
    logic_layer_default_handler,
    logic_layer_handler,
    logic_layer_non_unique_levels_handler,
    logic_layer_non_unique_levels_default_handler,
    flush_handler,
    index_handler,
    metadata_handler,
    metadata_all_handler,
    members_handler,
    members_default_handler,
};
use crate::logic_layer::{Cache, LogicLayerConfig};

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
    pub geoservice_url: Option<String>,
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
    pub cache: Arc<RwLock<Cache>>,
    pub logic_layer_config: Option<Arc<RwLock<LogicLayerConfig>>>,
}

/// Creates an ActixWeb application with an `AppState`.
pub fn create_app(
        debug: bool,
        backend: Box<dyn Backend + Sync + Send>,
        db_type: Database,
        env_vars: EnvVars,
        schema: Arc<RwLock<Schema>>,
        cache: Arc<RwLock<Cache>>,
        logic_layer_config: Option<Arc<RwLock<LogicLayerConfig>>>,
        streaming_response: bool,
        has_unique_levels_properties: bool,
    ) -> App<AppState>
{
    let app = App::with_state(AppState { debug, backend, db_type, env_vars, schema, cache, logic_layer_config })
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

        // Helpers
        .resource("/cubes/{cube}/members", |r| {
            r.method(Method::GET).with(members_default_handler)
        })
        .resource("/cubes/{cube}/members.{format}", |r| {
            r.method(Method::GET).with(members_handler)
        })

        .resource("/flush", |r| {
            r.method(Method::POST).with(flush_handler)
        })
        // Allow the API to accept /my-path or /my-path/ for all requests
        .default_resource(|r| r.h(NormalizePath::default()));

    let app = if streaming_response {
        app
            .resource("/cubes/{cube}/aggregate", |r| {
                r.method(Method::GET).with(aggregate_stream_default_handler)
            })
            .resource("/cubes/{cube}/aggregate.{format}", |r| {
                r.method(Method::GET).with(aggregate_stream_handler)
            })
    } else {
        app
            .resource("/cubes/{cube}/aggregate", |r| {
                r.method(Method::GET).with(aggregate_default_handler)
            })
            .resource("/cubes/{cube}/aggregate.{format}", |r| {
                r.method(Method::GET).with(aggregate_handler)
            })
    };

    if has_unique_levels_properties {
        // Logic Layer
        app
            .resource("/data", |r| {
                r.method(Method::GET).with(logic_layer_default_handler)
            })
            .resource("/data.{format}", |r| {
                r.method(Method::GET).with(logic_layer_handler)
            })

    } else {
        app
            .resource("/data", |r| {
                r.method(Method::GET).with(logic_layer_non_unique_levels_default_handler)
            })
            .resource("/data.{format}", |r| {
                r.method(Method::GET).with(logic_layer_non_unique_levels_handler)
            })
    }

}
