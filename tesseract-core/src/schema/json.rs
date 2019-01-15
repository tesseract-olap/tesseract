use serde_derive::Deserialize;

use crate::sql::MemberType;

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct SchemaConfigJSON {
    pub name: String,
    pub shared_dimensions: Option<Vec<SharedDimensionConfigJSON>>,
    pub cubes: Vec<CubeConfigJSON>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct CubeConfigJSON {
    pub name: String,
    pub table: TableConfigJSON,
    pub dimensions: Vec<DimensionConfigJSON>,
    pub dimension_usages: Option<Vec<DimensionUsageJSON>>,
    pub measures: Vec<MeasureConfigJSON>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct DimensionConfigJSON {
    pub name: String,
    pub foreign_key: Option<String>, // does not exist for shared dims
    pub hierarchies: Vec<HierarchyConfigJSON>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct SharedDimensionConfigJSON {
    pub name: String,
    pub hierarchies: Vec<HierarchyConfigJSON>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct DimensionUsageJSON {
    pub name: String,
    pub foreign_key: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct HierarchyConfigJSON {
    pub name: String,
    pub table: Option<TableConfigJSON>,
    pub primary_key: Option<String>,
    pub levels: Vec<LevelConfigJSON>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct LevelConfigJSON {
    pub name: String,
    pub key_column: String,
    pub name_column: Option<String>,
    pub properties: Option<Vec<PropertyConfigJSON>>,
    pub key_type: Option<MemberType>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct MeasureConfigJSON {
    pub name: String,
    pub column: String,
    pub aggregator: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TableConfigJSON {
    pub name: String,
    pub schema: Option<String>,
    pub primary_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct PropertyConfigJSON {
    pub name: String,
    pub column: String,
}
