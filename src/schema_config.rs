use failure:: Error;
use serde_json;

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct SchemaConfig {
    dimensions: Vec<DimensionConfig>,
    cubes: Vec<CubeConfig>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct CubeConfig {
    dimensions: Vec<DimensionConfig>,
    measures: Vec<MeasureConfig>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct DimensionConfig {
    name: String,
    key_column: String,
    name_column: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct MeasureConfig {
    name: String,
    key_column: String,
    name_column: String,
}

impl SchemaConfig {
    pub fn from_json(input: &str) -> Result<Self, Error> {
        serde_json::from_str(input)
            .map_err(|err| {
                format_err!("error reading json schema config: {}", err)
            })
    }
}

