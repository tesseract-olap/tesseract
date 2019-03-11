use failure::{Error, format_err};
use std::collections::HashSet;

use tesseract_core::Schema;


/// Reads a schema from an XML or JSON file and converts it into a `tesseract_core::Schema` object.
pub fn read_schema(schema_path: &String) -> Result<Schema, Error> {
    let schema_str = std::fs::read_to_string(&schema_path)
        .map_err(|_| format_err!("Schema file not found at {}", schema_path))?;

    let mut schema: Schema;

    if schema_path.ends_with("xml") {
        schema = Schema::from_xml(&schema_str)?;
    } else if schema_path.ends_with("json") {
        schema = Schema::from_json(&schema_str)?;
    } else {
        return Err(format_err!("Schema format not supported"))
    }

    // Check each cube for unique level and property names
    for cube in schema.cubes.clone() {
        let mut levels = HashSet::new();
        let mut properties = HashSet::new();

        for dimension in cube.dimensions.clone() {
            for hierarchy in dimension.hierarchies.clone() {
                for level in hierarchy.levels.clone() {
                    if levels.contains(&level.name) {
                        return Err(format_err!("Make sure the {} cube has unique level names", cube.name))
                    } else {
                        levels.insert(level.name);
                    }

                    match level.properties {
                        Some(props) => {
                            for property in props {
                                if properties.contains(&property.name) {
                                    return Err(format_err!("Make sure the {} cube has unique property names", cube.name))
                                } else {
                                    properties.insert(property.name);
                                }
                            }
                        },
                        None => continue
                    }
                }
            }
        }
    }

    Ok(schema)
}
