use serde_derive::Serialize;
use std::convert::From;

pub mod aggregator;
mod json;
mod xml;

pub use crate::schema::{
    json::SchemaConfigJson,
    json::DimensionConfigJson,
    json::HierarchyConfigJson,
    json::LevelConfigJson,
    json::MeasureConfigJson,
    json::TableConfigJson,
    json::PropertyConfigJson,
    xml::SchemaConfigXML,
    xml::DimensionConfigXML,
    xml::HierarchyConfigXML,
    xml::LevelConfigXML,
    xml::MeasureConfigXML,
    xml::TableConfigXML,
    xml::PropertyConfigXML,
};
use crate::names::{LevelName, Measure as MeasureName};
use crate::query_ir::MemberType;
pub use self::aggregator::Aggregator;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Schema {
    pub name: String,
    pub cubes: Vec<Cube>,
}

impl From<SchemaConfigJson> for Schema {
    fn from(schema_config: SchemaConfigJson) -> Self {
        // TODO
        // check for:
        // - duplicate cube names,
        // - duplicate dim names

        let mut cubes = vec![];

        for cube_config in schema_config.cubes {
            let mut dimensions: Vec<_> = cube_config.dimensions.into_iter()
                .map(|dim| dim.into())
                .collect();
            let measures = cube_config.measures.into_iter()
                .map(|mea| mea.into())
                .collect();

            // special case: check for dimension_usages
            if let Some(dim_usages) = cube_config.dimension_usages {
                for dim_usage in dim_usages {
                    if let Some(ref shared_dims) = schema_config.shared_dimensions {
                        for shared_dim_config in shared_dims {
                            if dim_usage.name == shared_dim_config.name {
                                let hierarchies = shared_dim_config.hierarchies.iter()
                                    .map(|h| h.clone().into())
                                    .collect();

                                dimensions.push(Dimension {
                                    name: shared_dim_config.name.clone(),
                                    foreign_key: Some(dim_usage.foreign_key.clone()),
                                    hierarchies: hierarchies,
                                });
                            }
                        }
                    }
                }
            }

            cubes.push(Cube {
                name: cube_config.name,
                table: cube_config.table.into(),
                can_aggregate: false,
                dimensions: dimensions,
                measures: measures,
            });
        }

        Schema {
            name: schema_config.name,
            cubes: cubes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Cube {
    pub name: String,
    pub table: Table,
    pub can_aggregate: bool,
    pub dimensions: Vec<Dimension>,
    pub measures: Vec<Measure>,
}

impl Cube {
    /// Returns a Vec<String> of all the dimension name options for a given Cube.
    pub fn get_all_level_names(&self) -> Vec<LevelName> {
        let mut dimension_names: Vec<LevelName> = vec![];

        for dimension in &self.dimensions {
            let dimension_name = dimension.name.clone();
            for hierarchy in &dimension.hierarchies {
                let hierarchy_name = hierarchy.name.clone();
                for level in &hierarchy.levels {
                    let level_name = level.name.clone();
                    dimension_names.push(
                        LevelName {
                            dimension: dimension_name.clone(),
                            hierarchy: hierarchy_name.clone(),
                            level: level_name,
                        }
                    );
                }
            }
        }

        dimension_names
    }

    /// Returns a Vec<String> of all the measure names for a given Cube.
    pub fn get_all_measure_names(&self) -> Vec<MeasureName> {
        let mut measure_names: Vec<MeasureName> = vec![];

        for measure in &self.measures {
            measure_names.push(
                MeasureName::new(measure.name.clone())
            );
        }

        measure_names
    }
}


#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Dimension {
    pub name: String,
    pub foreign_key: Option<String>,
    pub hierarchies: Vec<Hierarchy>,
}

impl From<DimensionConfigJson> for Dimension {
    fn from(dimension_config: DimensionConfigJson) -> Self {
        let hierarchies = dimension_config.hierarchies.into_iter()
            .map(|h| h.into())
            .collect();

        Dimension {
            name: dimension_config.name,
            foreign_key: dimension_config.foreign_key,
            hierarchies: hierarchies,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Hierarchy {
    pub name: String,
    pub table: Option<Table>,
    pub primary_key: String,
    pub levels: Vec<Level>,
}

impl From<HierarchyConfigJson> for Hierarchy {
    fn from(hierarchy_config: HierarchyConfigJson) -> Self {
        let levels: Vec<Level> = hierarchy_config.levels.into_iter()
            .map(|l| l.into())
            .collect();

        let primary_key = hierarchy_config.primary_key
            .unwrap_or_else(|| {
                levels.iter()
                    .last()
                    .expect("TODO check that there's at least 1 level")
                    .key_column
                    .clone()
            });

        Hierarchy {
            name: hierarchy_config.name,
            table: hierarchy_config.table.map(|t| t.into()),
            primary_key: primary_key,
            levels: levels,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Level {
    pub name: String,
    pub key_column: String,
    pub name_column: Option<String>,
    pub properties: Option<Vec<Property>>,
    pub key_type: Option<MemberType>,
}

impl From<LevelConfigJson> for Level {
    fn from(level_config: LevelConfigJson) -> Self {
        let properties = level_config.properties
            .map(|ps| {
                ps.into_iter()
                    .map(|p| p.into())
                    .collect()
            });

        Level {
            name: level_config.name,
            key_column: level_config.key_column,
            name_column: level_config.name_column,
            properties,
            key_type: level_config.key_type,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Measure{
    pub name: String,
    pub column: String,
    pub aggregator: Aggregator,
}

impl From<MeasureConfigJson> for Measure {
    fn from(measure_config: MeasureConfigJson) -> Self {
        Measure {
            name: measure_config.name,
            column: measure_config.column,
            aggregator: measure_config.aggregator,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Table{
    pub name: String,
    pub schema: Option<String>,
    pub primary_key: Option<String>,
}

impl From<TableConfigJson> for Table {
    fn from(table_config: TableConfigJson) -> Self {
        Table {
            name: table_config.name,
            schema: table_config.schema,
            primary_key: table_config.primary_key,
        }
    }
}

impl Table {
    pub fn full_name(&self) -> String {
        if let Some(ref schema) = self.schema {
            format!("{}.{}", schema, self.name)
        } else {
            self.name.to_owned()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Property{
    pub name: String,
    pub column: String,
}

impl From<PropertyConfigJson> for Property {
    fn from(property_config: PropertyConfigJson) -> Self {
        Property {
            name: property_config.name,
            column: property_config.column,
        }
    }
}
