use std::sync::{Arc, Mutex};

pub type Schema = Arc<Mutex<SchemaData>>;

pub fn init(schema_data: SchemaData) -> Schema {
    Arc::new(Mutex::new(schema_data))
}

// replaces the schema inside of the Arc Mutex
// with another one read from scratch
#[allow(dead_code)]
pub fn flush(schema: Schema, schema_data: SchemaData) {
    let mut data = schema.lock().unwrap();
    *data = schema_data;
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SchemaData {
    cubes: Vec<Cube>,
}

impl SchemaData {
    pub fn new() -> Self {
        SchemaData {
            cubes: vec![],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Cube {
    can_aggregate: bool,
    dimensions: Vec<String>,
    measures: Vec<String>,
}
