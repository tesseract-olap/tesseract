use std::sync::{Arc, Mutex};

pub type Schema = Arc<Mutex<SchemaData>>;

pub fn init() -> Schema {
    Arc::new(Mutex::new(SchemaData::new()))
}

// replaces the schema inside of the Arc Mutex
// with another one read from scratch
pub fn flush(schema: Schema) {

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
