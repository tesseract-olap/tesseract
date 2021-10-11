use actix_web::{
    web,
    //http::NormalizePath,
};
use tesseract_core::{Backend, Schema, CubeHasUniqueLevelsAndProperties};
use crate::db_config::Database;
use crate::handlers::{
    aggregate,
    //aggregate_stream,
    diagnosis,
    flush,
    index,
    //logic_layer,
    metadata,
};
use crate::logic_layer::{Cache, LogicLayerConfig};

use std::sync::{Arc, RwLock};
use url::Url;
use r2d2_redis::{r2d2, RedisConnectionManager};


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
    pub geoservice_url: Option<Url>,
    pub schema_source: SchemaSource,
    pub jwt_secret: Option<String>,
    pub flush_secret: Option<String>,
}

/// Holds [ActixWeb State](https://actix.rs/docs/application/).
#[derive(Clone)]
pub struct AppState {
    pub debug: bool,
    pub backend: Box<dyn Backend + Sync + Send>,
    pub redis_pool: Option<r2d2::Pool<RedisConnectionManager>>,
    // TODO this is a hack, until a better interface is set up with the Backend Trait
    // to generate its own sql.
    pub db_type: Database,
    pub env_vars: EnvVars,
    pub schema: Arc<RwLock<Schema>>,
    pub cache: Arc<RwLock<Cache>>,
    pub logic_layer_config: Option<Arc<RwLock<LogicLayerConfig>>>,
    // TODO is there a way to access this that's not through state? Tried using closures to
    // capture, but the handlers need to implement Fn, not FnOnce (which happens once capturing
    // variables from environment
    pub has_unique_levels_properties: CubeHasUniqueLevelsAndProperties,
    pub no_logic_layer: bool,
}

/// Creates an ActixWeb application with an `AppState`.
pub fn config_app(
        cfg: &mut web::ServiceConfig,
        appstate: AppState,
        streaming_response: bool,
    )
{
    let app = cfg;

    app
        .data(appstate)
        // Metadata
        .route("/", web::get().to(index::handler))
        .route("/cubes", web::get().to(metadata::all_handler))
        .route("/cubes/{cube}", web::get().to(metadata::handler))

        // Helpers
        .route("/cubes/{cube}/members",web::get().to(metadata::members_default_handler))
        .route("/cubes/{cube}/members.{format}", web::get().to(metadata::members_handler))

        // Data Quality Assurance
        .route("/diagnosis", web::get().to(diagnosis::default_handler))
        .route("/diagnosis.{format}", web::get().to(diagnosis::handler))
        .route("/flush", web::post().to(flush::handler));
        // Allow the API to accept /my-path or /my-path/ for all requests
        //.default_resource(|r| r.h(NormalizePath::default()));

    if streaming_response {
        app
        //    .route("/cubes/{cube}/aggregate", web::get().to(aggregate_stream_default_handler))
        //    .route("/cubes/{cube}/aggregate.{format}", web::get().to(aggregate_stream_handler))
    } else {
        app
            .route("/cubes/{cube}/aggregate", web::get().to(aggregate::default_handler))
            .route("/cubes/{cube}/aggregate.{format}", web::get().to(aggregate::handler))
    };

    //match has_unique_levels_properties {
    //    CubeHasUniqueLevelsAndProperties::True => {
    //        // Logic Layer
    //        app
    //            .route("/data", web::get().to(logic_layer_default_handler))
    //            .route("/data.{format}", web::get().to(logic_layer_handler))
    //            .route("/members", web::get().to(logic_layer_members_default_handler))
    //            .route("/members.{format}", web::get().to(logic_layer_members_handler))
    //            .route("/relations", web::get().to(logic_layer_relations_default_handler))
    //            .route("/relations.{foramt}", web::get().to(logic_layer_relations_handler))
    //    },
    //    CubeHasUniqueLevelsAndProperties::False { .. } => {
    //        // No Logic Layer, give error instead
    //        app
    //            .route("/data", web::get().to(logic_layer_non_unique_levels_default_handler))
    //            .route("/data.{format}", web::get().to(logic_layer_non_unique_levels_handler))
    //            .route("/members", web::get().to(logic_layer_non_unique_levels_default_handler))
    //            .route("/members.{format}", web::get().to(logic_layer_non_unique_levels_handler))
    //            .route("/relations", web::get().to(logic_layer_relations_non_unique_levels_default_handler))
    //            // FIXME format typo
    //            .route("/relations.{foramt}", web::get().to(logic_layer_relations_non_unique_levels_handler))
    //    },
    //};
}
