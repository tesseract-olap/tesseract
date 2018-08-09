use std::convert::From;

use ::schema_config::{
    SchemaConfig,
    DimensionConfig,
    HierarchyConfig,
    LevelConfig,
    MeasureConfig,
    TableConfig,
};

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Schema {
    pub name: String,
    pub cubes: Vec<Cube>,
}

impl From<SchemaConfig> for Schema {
    fn from(schema_config: SchemaConfig) -> Self {
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
            for dim_usage in cube_config.dimension_usages {
                for shared_dim_config in &schema_config.shared_dimensions {
                    if dim_usage.name == shared_dim_config.name {
                        let hierarchies = shared_dim_config.hierarchies.iter()
                            .map(|h| h.clone().into())
                            .collect();

                        dimensions.push(Dimension {
                            name: shared_dim_config.name.clone(),
                            foreign_key: dim_usage.foreign_key.clone(),
                            hierarchies: hierarchies,

                        });
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

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Dimension {
    pub name: String,
    pub foreign_key: String,
    pub hierarchies: Vec<Hierarchy>,
}

impl From<DimensionConfig> for Dimension {
    fn from(dimension_config: DimensionConfig) -> Self {
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
    pub table: Table,
    pub primary_key: String,
    pub levels: Vec<Level>,
}

impl From<HierarchyConfig> for Hierarchy {
    fn from(hierarchy_config: HierarchyConfig) -> Self {
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
            table: hierarchy_config.table.into(),
            primary_key: primary_key,
            levels: levels,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Level {
    pub name: String,
    pub key_column: String,
    pub name_column: String,
}

impl From<LevelConfig> for Level {
    fn from(level_config: LevelConfig) -> Self {
        Level {
            name: level_config.name,
            key_column: level_config.key_column,
            name_column: level_config.name_column,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Measure{
    pub name: String,
    pub column: String,
}

impl From<MeasureConfig> for Measure {
    fn from(measure_config: MeasureConfig) -> Self {
        Measure {
            name: measure_config.name,
            column: measure_config.column,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Table{
    pub name: String,
    pub schema: String,
}

impl From<TableConfig> for Table {
    fn from(table_config: TableConfig) -> Self {
        Table {
            name: table_config.name,
            schema: table_config.schema,
        }
    }
}


