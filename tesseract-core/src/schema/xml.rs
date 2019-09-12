//! XML files first get deserialized using the structs in this file.
//! Then they get serialized into a JSON string and deserialized again using
//! the structs in the json.rs file.
//!
//! This is done for two reasons:
//! 1. XML files have different key names for vector fields, so we need a way
//!    to tell Serde what those key names are.
//! 2. It avoids having to implement the same Schema traits multiple times for
//!    JSON and XML configs. Instead, only the JSON config traits are implemented
//!    so everything needs to be converted into a SchemaConfigJSON for now.

use serde_derive::Deserialize;
use serde_derive::Serialize;

use crate::query_ir::MemberType;
use super::aggregator::Aggregator;
use super::{DimensionType, MeasureType};


#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct SchemaConfigXML {
    pub name: String,
    #[serde(rename(deserialize="SharedDimension"))]
    pub shared_dimensions: Option<Vec<SharedDimensionConfigXML>>,
    #[serde(rename(deserialize="Cube"))]
    pub cubes: Vec<CubeConfigXML>,
    #[serde(rename(deserialize="Annotation"))]
    pub annotations: Option<Vec<AnnotationConfigXML>>,
    pub default_locale: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct CubeConfigXML {
    pub name: String,
    #[serde(rename(deserialize="Table"))]
    pub table: TableConfigXML,
    #[serde(rename(deserialize="Dimension"))]
    pub dimensions: Option<Vec<DimensionConfigXML>>,
    #[serde(rename(deserialize="DimensionUsage"))]
    pub dimension_usages: Option<Vec<DimensionUsageXML>>,
    #[serde(rename(deserialize="Measure"))]
    pub measures: Vec<MeasureConfigXML>,
    #[serde(rename(deserialize="Annotation"))]
    pub annotations: Option<Vec<AnnotationConfigXML>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct DimensionConfigXML {
    pub name: String,
    pub foreign_key: Option<String>, // does not exist for shared dims
    #[serde(rename(deserialize="Hierarchy"))]
    pub hierarchies: Vec<HierarchyConfigXML>,
    pub default_hierarchy: Option<String>,
    #[serde(rename="type")]
    pub dim_type: Option<DimensionType>,
    #[serde(rename(deserialize="Annotation"))]
    pub annotations: Option<Vec<AnnotationConfigXML>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct SharedDimensionConfigXML {
    pub name: String,
    #[serde(rename(deserialize="Hierarchy"))]
    pub hierarchies: Vec<HierarchyConfigXML>,
    pub default_hierarchy: Option<String>,
    #[serde(rename="type")]
    pub dim_type: Option<DimensionType>,
    #[serde(rename(deserialize="Annotation"))]
    pub annotations: Option<Vec<AnnotationConfigXML>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct DimensionUsageXML {
    pub source: String,
    pub name: Option<String>,
    pub foreign_key: String,
    #[serde(rename(deserialize="Annotation"))]
    pub annotations: Option<Vec<AnnotationConfigXML>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct HierarchyConfigXML {
    pub name: String,
    #[serde(rename(deserialize="Table"))]
    pub table: Option<TableConfigXML>,
    pub primary_key: Option<String>,
    #[serde(rename(deserialize="Level"))]
    pub levels: Vec<LevelConfigXML>,
    #[serde(rename(deserialize="Annotation"))]
    pub annotations: Option<Vec<AnnotationConfigXML>>,
    #[serde(rename(deserialize="InlineTable"))]
    pub inline_table: Option<InlineTableXML>,
    pub default_member: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct InlineTableXML {
    pub alias: String,
    #[serde(rename(deserialize="ColumnDef"))]
    pub column_definitions: Vec<InlineTableColumnDefinitionXML>,
    #[serde(rename(deserialize="Row"))]
    pub rows: Vec<InlineTableRowXML>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct InlineTableColumnDefinitionXML {
    pub name: String,
    pub key_type: MemberType,
    pub key_column_type: Option<String>,
    pub caption_set: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct InlineTableRowXML {
    #[serde(rename(deserialize="Value"))]
    pub row_values: Vec<InlineTableRowValueXML>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct InlineTableRowValueXML {
    pub column: String,
    #[serde(rename(deserialize="$value"))]
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct LevelConfigXML {
    pub name: String,
    pub key_column: String,
    pub name_column: Option<String>,
    #[serde(rename(deserialize="Property"))]
    pub properties: Option<Vec<PropertyConfigXML>>,
    pub key_type: Option<MemberType>,
    #[serde(rename(deserialize="Annotation"))]
    pub annotations: Option<Vec<AnnotationConfigXML>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct MeasureConfigXML {
    pub name: String,
    pub column: String,
    pub aggregator: Aggregator,
    #[serde(rename="type")]
    pub measure_type: Option<MeasureType>,
    #[serde(rename(deserialize="Annotation"))]
    pub annotations: Option<Vec<AnnotationConfigXML>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct TableConfigXML {
    pub name: String,
    pub schema: Option<String>,
    pub primary_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct PropertyConfigXML {
    pub name: String,
    pub column: String,
    pub caption_set: Option<String>,
    #[serde(rename(deserialize="Annotation"))]
    pub annotations: Option<Vec<AnnotationConfigXML>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct AnnotationConfigXML {
    pub name: String,
    #[serde(rename(deserialize="$value"))]
    pub text: String,
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_xml_rs::from_reader;

    #[test]
    fn xml_schema_config() {
        let s = r##"
            <Schema name="my_schema">
                <SharedDimension name="Geo">
                    <Hierarchy name="Geo">
                        <Level name="Tract" key_column="geoid" />
                    </Hierarchy>
                </SharedDimension>
                <Cube name="my_cube">
                    <Table name="my_table" />
                    <Dimension name="my_dim">
                        <Hierarchy name="my_hier">
                            <Level name="my_level" key_column="key" />
                        </Hierarchy>
                    </Dimension>
                    <Measure name="my_mea" column="mea" aggregator="sum" />
                </Cube>
            </Schema>
        "##;
        let xml_schema_config: SchemaConfigXML = from_reader(s.as_bytes()).unwrap();
    }
}
