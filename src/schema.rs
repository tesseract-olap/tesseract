use failure::Error;
use serde_json;
use std::sync::{Arc, RwLock};

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

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct SchemaData {
    cubes: Vec<Cube>,
}

impl SchemaData {
    pub fn from_json(input: &str) -> Result<Self, Error> {
        serde_json::from_str(input)
            .map_err(|err| {
                format_err!("error reading json schema: {}", err)
            })
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Cube {
    can_aggregate: bool,
    dimensions: Vec<String>,
    measures: Vec<String>,
}
