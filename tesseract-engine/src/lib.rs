extern crate csv;
#[macro_use]
extern crate failure;
#[macro_use]
extern crate indexmap;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate xxhash2;

pub mod backends;
mod schema;
mod schema_config;
pub mod query;

use failure::Error;

use schema::Schema;
use schema_config::SchemaConfig;

#[derive(Debug, Clone)]
pub struct TesseractEngine {
    pub schema: Schema,
}

impl TesseractEngine {
    pub fn from_json(raw_schema: &str) -> Result<Self, Error> {
        let schema_config = serde_json::from_str::<SchemaConfig>(raw_schema)?;
        Ok(TesseractEngine {
            schema: schema_config.into(),
        })
    }

    pub fn cubes_metadata(&self) -> Schema {
        self.schema.clone()
    }

    pub fn flush(&mut self, raw_schema: &str) -> Result<(), Error> {
        let schema_config = serde_json::from_str::<SchemaConfig>(raw_schema)?;
        self.schema = schema_config.into();
        Ok(())
    }
}
