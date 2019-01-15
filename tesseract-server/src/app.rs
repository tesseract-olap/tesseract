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


pub struct AppState {
    pub backend: Box<dyn Backend + Sync + Send>,
    // TODO this is a hack, until a better interface is set up with the Backend Trait
    // to generate its own sql.
    pub db_type: Database,
    pub schema: Arc<RwLock<Schema>>,
}

pub fn create_app(backend: Box<dyn Backend + Sync + Send>, db_type: Database, schema: Arc<RwLock<Schema>>) -> App<AppState> {
    App::with_state(AppState { backend, db_type, schema })
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
