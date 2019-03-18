use serde_derive::Deserialize;

use crate::query_ir::MemberType;
use super::aggregator::Aggregator;

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct SchemaConfigJson {
    pub name: String,
    pub shared_dimensions: Option<Vec<SharedDimensionConfigJson>>,
    pub cubes: Vec<CubeConfigJson>,
    pub annotations: Option<Vec<AnnotationConfigJson>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct CubeConfigJson {
    pub name: String,
    pub table: TableConfigJson,
    pub dimensions: Vec<DimensionConfigJson>,
    pub dimension_usages: Option<Vec<DimensionUsageJson>>,
    pub measures: Vec<MeasureConfigJson>,
    pub annotations: Option<Vec<AnnotationConfigJson>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct DimensionConfigJson {
    pub name: String,
    pub foreign_key: Option<String>, // does not exist for shared dims
    pub hierarchies: Vec<HierarchyConfigJson>,
    pub annotations: Option<Vec<AnnotationConfigJson>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct SharedDimensionConfigJson {
    pub name: String,
    pub hierarchies: Vec<HierarchyConfigJson>,
    pub annotations: Option<Vec<AnnotationConfigJson>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct DimensionUsageJson {
    pub name: String,
    pub foreign_key: String,
    pub annotations: Option<Vec<AnnotationConfigJson>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct HierarchyConfigJson {
    pub name: String,
    pub table: Option<TableConfigJson>,
    pub primary_key: Option<String>,
    pub levels: Vec<LevelConfigJson>,
    pub annotations: Option<Vec<AnnotationConfigJson>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct LevelConfigJson {
    pub name: String,
    pub key_column: String,
    pub name_column: Option<String>,
    pub properties: Option<Vec<PropertyConfigJson>>,
    pub key_type: Option<MemberType>,
    pub annotations: Option<Vec<AnnotationConfigJson>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct MeasureConfigJson {
    pub name: String,
    pub column: String,
    pub aggregator: Aggregator,
    pub annotations: Option<Vec<AnnotationConfigJson>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TableConfigJson {
    pub name: String,
    pub schema: Option<String>,
    pub primary_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct PropertyConfigJson {
    pub name: String,
    pub column: String,
    pub caption_set: Option<String>,
    pub annotations: Option<Vec<AnnotationConfigJson>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct AnnotationConfigJson {
    pub name: String,
    pub text: String,
}

