use failure:: Error;
use serde_json;

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct SchemaConfig {
    pub name: String,
    pub shared_dimensions: Vec<SharedDimensionConfig>,
    pub cubes: Vec<CubeConfig>,
}

impl SchemaConfig {
    pub fn from_json(input: &str) -> Result<Self, Error> {
        serde_json::from_str(input)
            .map_err(|err| {
                format_err!("error reading json schema config: {}", err)
            })
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct CubeConfig {
    pub name: String,
    pub dimensions: Vec<DimensionConfig>,
    pub dimension_usages: Vec<DimensionUsage>,
    pub measures: Vec<MeasureConfig>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct DimensionConfig {
    pub name: String,
    pub foreign_key: String, // does not exist for shared dims
    pub hierarchies: Vec<HierarchyConfig>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct SharedDimensionConfig {
    pub name: String,
    pub hierarchies: Vec<HierarchyConfig>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct DimensionUsage {
    pub name: String,
    pub foreign_key: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct HierarchyConfig {
    pub name: String,
    pub primary_key: Option<String>,
    pub levels: Vec<LevelConfig>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct LevelConfig {
    pub name: String,
    pub key_column: String,
    pub name_column: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct MeasureConfig {
    pub name: String,
    pub column: String,
}

