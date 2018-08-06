use failure::Error;
use std::sync::{Arc, RwLock};

use ::schema_config::SchemaConfig;

pub type Schema = Arc<RwLock<SchemaData>>;

pub fn init(schema_data: SchemaData) -> Schema {
    Arc::new(RwLock::new(schema_data))
}

// replaces the schema inside of the Arc Mutex
// with another one read from scratch
#[allow(dead_code)]
pub fn flush(schema: Schema, schema_data: SchemaData) {
    let mut data = schema.write().unwrap();
    *data = schema_data;
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SchemaData {
    cubes: Vec<Cube>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Cube {
    can_aggregate: bool,
    dimensions: Vec<String>,
    measures: Vec<String>,
}

impl SchemaData {
    pub fn from_config(schema_config: &SchemaConfig) -> SchemaData {
        SchemaData {
            cubes: vec![],
        }
    }
}
