use actix_web::{
    http::Method,
    middleware,
    App,
};
use clickhouse_rs:: Options as ChOptions;
use tesseract_core::Schema;

use crate::handlers::{
    index_handler,
    aggregate_handler,
};

pub struct AppState {
    pub clickhouse_options: ChOptions,
    pub schema: Schema,
}

pub fn create_app(clickhouse_options: ChOptions, schema: Schema) -> App<AppState> {
    App::with_state(AppState { clickhouse_options, schema })
        .middleware(middleware::Logger::default())
        .resource("/", |r| {
            r.method(Method::GET).with(index_handler)
        })
        .resource("/cubes/{cube}/aggregate{format}", |r| {
            r.method(Method::GET).with(aggregate_handler)
        })
}
