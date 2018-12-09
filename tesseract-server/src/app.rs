use actix_web::{
    http::Method,
    middleware,
    App,
};
use tesseract_core::{Backend, Schema};

use crate::handlers::{
    aggregate_handler,
    aggregate_default_handler,
    index_handler,
    metadata_handler,
    metadata_all_handler,
};

pub struct AppState {
    pub backend: Box<dyn Backend + Sync + Send>,
    pub schema: Schema,
}

pub fn create_app(backend: Box<dyn Backend + Sync + Send>, schema: Schema) -> App<AppState> {
    App::with_state(AppState { backend, schema })
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
}
