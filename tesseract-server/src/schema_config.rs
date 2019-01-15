use failure::{Error, format_err};
use std::env;

use tesseract_core::Schema;


pub fn read_schema() -> Result<Schema, Error> {
    let schema_path = env::var("TESSERACT_SCHEMA_FILEPATH")
        .expect("TESSERACT_SCHEMA_FILEPATH not found");

    let schema_str = std::fs::read_to_string(&schema_path)
        .map_err(|_| format_err!("Schema file not found at {}", schema_path))?;

    let mut schema: Schema;

    if schema_path.ends_with("xml") {
        schema = Schema::from_xml(&schema_str)?;
    } else if schema_path.ends_with("json") {
        schema = Schema::from_json(&schema_str)?;
    } else {
        panic!("Schema format not supported");
    }

    Ok(schema)
}
