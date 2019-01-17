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

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct SchemaConfigXML {
    pub name: String,
    #[serde(rename(deserialize="SharedDimension"))]
    pub shared_dimensions: Option<Vec<SharedDimensionConfigXML>>,
    #[serde(rename(deserialize="Cube"))]
    pub cubes: Vec<CubeConfigXML>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct CubeConfigXML {
    pub name: String,
    #[serde(rename(deserialize="Table"))]
    pub table: TableConfigXML,
    #[serde(rename(deserialize="Dimension"))]
    pub dimensions: Vec<DimensionConfigXML>,
    #[serde(rename(deserialize="DimensionUsage"))]
    pub dimension_usages: Option<Vec<DimensionUsageXML>>,
    #[serde(rename(deserialize="Measure"))]
    pub measures: Vec<MeasureConfigXML>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct DimensionConfigXML {
    pub name: String,
    pub foreign_key: Option<String>, // does not exist for shared dims
    #[serde(rename(deserialize="Hierarchy"))]
    pub hierarchies: Vec<HierarchyConfigXML>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct SharedDimensionConfigXML {
    pub name: String,
    #[serde(rename(deserialize="Hierarchy"))]
    pub hierarchies: Vec<HierarchyConfigXML>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct DimensionUsageXML {
    pub name: String,
    pub foreign_key: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct HierarchyConfigXML {
    pub name: String,
    pub table: Option<TableConfigXML>,
    pub primary_key: Option<String>,
    #[serde(rename(deserialize="Level"))]
    pub levels: Vec<LevelConfigXML>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct LevelConfigXML {
    pub name: String,
    pub key_column: String,
    pub name_column: Option<String>,
    #[serde(rename(deserialize="Property"))]
    pub properties: Option<Vec<PropertyConfigXML>>,
    pub key_type: Option<MemberType>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct MeasureConfigXML {
    pub name: String,
    pub column: String,
    pub aggregator: String,
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
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn xml_schema_config() {
        let s = r##"
            <Schema name="my_schema">
                <Cube name="my_cube">
                    <Table name="my_table" />
                </Cube>
            </Schema>
        "##;
        let schema_config: SchemaConfigXML = serde_xml::deserialize(s.as_bytes()).unwrap();
    }
}
