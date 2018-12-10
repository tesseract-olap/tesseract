use failure:: {Error, format_err};
use serde_derive::Deserialize;
use serde_json;

use crate::sql::MemberType;

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct SchemaConfig {
    pub name: String,
    pub shared_dimensions: Option<Vec<SharedDimensionConfig>>,
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
    pub table: TableConfig,
    pub dimensions: Vec<DimensionConfig>,
    pub dimension_usages: Option<Vec<DimensionUsage>>,
    pub measures: Vec<MeasureConfig>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct DimensionConfig {
    pub name: String,
    pub foreign_key: Option<String>, // does not exist for shared dims
    pub foreign_key_type: Option<MemberType>,
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
    pub table: Option<TableConfig>,
    pub primary_key: Option<String>,
    pub levels: Vec<LevelConfig>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct LevelConfig {
    pub name: String,
    pub key_column: String,
    pub name_column: Option<String>,
    pub properties: Option<Vec<PropertyConfig>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct MeasureConfig {
    pub name: String,
    pub column: String,
    pub aggregator: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TableConfig {
    pub name: String,
    pub schema: Option<String>,
    pub primary_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct PropertyConfig {
    pub name: String,
    pub column: String,
}

