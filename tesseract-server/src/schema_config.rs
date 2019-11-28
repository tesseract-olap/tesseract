use failure::{Error, format_err};

use tesseract_core::Schema;
use crate::app::{AppState, SchemaSource, EnvVars};

pub fn reload_schema(schema_config: &SchemaSource) -> Result<Schema, Error> {
    match schema_config {
        SchemaSource::LocalSchema { ref filepath } => {
            let (content, mode) = self::file_path_to_string_mode(filepath).expect("parse fail");
            read_schema(&content, &mode)
        },
        _ => panic!("Invalid schema type!")
    }
}

pub fn file_path_to_string_mode(schema_path: &String) -> Result<(String, String), Error> {
    let schema_str = std::fs::read_to_string(&schema_path)
        .map_err(|_| format_err!("Schema file not found at {}", schema_path))?;
    let mode = match schema_path.ends_with("xml") {
        true => "xml",
        _ => "json"
    };
    Ok((schema_str, mode.to_string()))
}

/// Reads a schema from an XML or JSON file and converts it into a `tesseract_core::Schema` object.
pub fn read_schema(schema_content: &String, mode: &String) -> Result<Schema, Error> {

    let schema = if mode == "xml" {
        Schema::from_xml(&schema_content)?
    } else if mode == "json" {
        Schema::from_json(&schema_content)?
    } else {
        return Err(format_err!("Schema format not supported"))
    };
    // TODO Should this check be done in core?
    for cube in &schema.cubes {
        for dimension in &cube.dimensions {
            for hierarchy in &dimension.hierarchies {
                let has_table = hierarchy.table.is_some();
                let has_inline_table = hierarchy.inline_table.is_some();

                if has_table && has_inline_table {
                    return Err(format_err!("Can't have table and inline table definitions in the same hierarchy"))
                }
            }
        }
    }

    Ok(schema)
}
