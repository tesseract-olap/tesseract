use failure::{Error, format_err};

use tesseract_core::{Schema, Backend};
use tesseract_core::schema::{SchemaConfigJson, json::SharedDimensionConfigJson, json::AnnotationConfigJson};
use crate::app::{AppState, SchemaSource, EnvVars};
use log::{info};
use futures::future::Future;

pub fn merge_schemas(schemas: &Vec<String>) -> String {
    // TODO add logic to merge JSON schemas here!
    let mut schema_objs: Vec<SchemaConfigJson> = schemas.into_iter().map(|raw_json| {
        serde_json::from_str(raw_json).unwrap()
    }).collect();

    // Take the first cube in the list to use as a basis. split off rest of the list

    let (mut master_obj, elements) = schema_objs.split_first_mut().unwrap();
    for obj in elements {
            // Copy cubes
            master_obj.cubes.extend(obj.cubes.iter().cloned());
            // Copy shared dimensions
            if obj.shared_dimensions.is_some() {
                if master_obj.shared_dimensions.is_none() {
                    master_obj.shared_dimensions = obj.shared_dimensions.clone();
                } else {
                    let mut new_shared: Vec<SharedDimensionConfigJson> = vec![];

                    let master_shared = master_obj.shared_dimensions.as_ref();
                    match master_shared {
                        Some(vals) => {
                            for v in vals {
                                new_shared.push(v.clone());
                            }
                        },
                        None => {}
                    }

                    let other_shared = obj.shared_dimensions.as_ref();
                    match other_shared {
                        Some(vals) => {
                            for v in vals {
                                new_shared.push(v.clone());
                            }
                        },
                        None => {}
                    }
                    master_obj.shared_dimensions = Some(new_shared);
                }
            }
            if obj.annotations.is_some() {
                if master_obj.annotations.is_none() {
                    master_obj.annotations = obj.annotations.clone();
                } else {
                    let mut new_shared: Vec<AnnotationConfigJson> = vec![];

                    let master_shared = master_obj.annotations.as_ref();
                    match master_shared {
                        Some(vals) => {
                            for v in vals {
                                new_shared.push(v.clone());
                            }
                        },
                        None => {}
                    }

                    let other_shared = obj.annotations.as_ref();
                    match other_shared {
                        Some(vals) => {
                            for v in vals {
                                new_shared.push(v.clone());
                            }
                        },
                        None => {}
                    }
                    master_obj.annotations = Some(new_shared);
                }
            }
    }

    let tmp_str = serde_json::to_string(master_obj).expect("Failed to serialize schema JSON to string");
    println!("HERE\n{}\n", tmp_str);
    tmp_str
}

pub fn reload_schema(schema_config: &SchemaSource, backend: Box<dyn Backend + Sync + Send>) -> Box<dyn Future<Item=Vec<String>, Error=Error>> {
    match schema_config {
        // SchemaSource::LocalSchema { ref filepath } => {
            // let (content, mode) = self::file_path_to_string_mode(filepath).expect("parse fail");
            // read_schema(&content, &mode)
        // },
        SchemaSource::DbSchema { ref tablepath } => {
            info!("Reading Schema from DB...");
            let schemas = backend.retrieve_schemas(&tablepath);
            // let content = merge_schemas(&schemas);
            // read_schema(&content, &"json".to_string())
            schemas
        },
        _ => panic!("Unsupported schema type!")
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
