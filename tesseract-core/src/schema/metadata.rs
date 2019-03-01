use serde_derive::Serialize;
use std::convert::From;

use super::{
    Schema,
    Cube,
    Dimension,
    Hierarchy,
    Level,
    Measure,
    Property,
    aggregator::Aggregator,
};

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SchemaMetadata {
    pub name: String,
    pub cubes: Vec<CubeMetadata>,
}

impl From<&Schema> for SchemaMetadata {
    fn from(schema: &Schema) -> Self {
        SchemaMetadata {
            name: schema.name.clone(),
            cubes: schema.cubes.iter().map(|c| c.into()).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CubeMetadata {
    pub name: String,
    pub dimensions: Vec<DimensionMetadata>,
    pub measures: Vec<MeasureMetadata>,
}

impl From<&Cube> for CubeMetadata {
    fn from(cube: &Cube) -> Self {
        CubeMetadata {
            name: cube.name.clone(),
            dimensions: cube.dimensions.iter().map(|d| d.into()).collect(),
            measures: cube.measures.iter().map(|m| m.into()).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DimensionMetadata {
    pub name: String,
    pub hierarchies: Vec<HierarchyMetadata>,
}

impl From<&Dimension> for DimensionMetadata {
    fn from(dimension: &Dimension) -> Self {
        DimensionMetadata {
            name: dimension.name.clone(),
            hierarchies: dimension.hierarchies.iter().map(|h| h.into()).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HierarchyMetadata {
    pub name: String,
    pub levels: Vec<LevelMetadata>,
}

impl From<&Hierarchy> for HierarchyMetadata {
    fn from(hierarchy: &Hierarchy) -> Self {
        HierarchyMetadata {
            name: hierarchy.name.clone(),
            levels: hierarchy.levels.iter().map(|l| l.into()).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LevelMetadata {
    pub name: String,
    pub properties: Option<Vec<PropertyMetadata>>,
}

impl From<&Level> for LevelMetadata {
    fn from(level: &Level) -> Self {
        let properties = level.properties.clone().map(|props| {
                props.iter().map(|p| p.into()).collect()
            });

        LevelMetadata {
            name: level.name.clone(),
            properties,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MeasureMetadata {
    pub name: String,
    pub aggregator: AggregatorMetadata,
}

impl From<&Measure> for MeasureMetadata {
    fn from(measure: &Measure) -> Self {
        MeasureMetadata {
            name: measure.name.clone(),
            aggregator: (&measure.aggregator).into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PropertyMetadata {
    pub name: String,
}

impl From<&Property> for PropertyMetadata {
    fn from(property: &Property) -> Self {
        PropertyMetadata {
            name: property.name.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AggregatorMetadata {
    pub name: String,
}

impl From<&Aggregator> for AggregatorMetadata {
    fn from(aggregator: &Aggregator) -> Self {
        let name = match *aggregator {
            Aggregator::Sum => "sum".into(),
            Aggregator::Count => "count".into(),
            Aggregator::Average => "avg".into(),
            Aggregator::Median => "median".into(),
            Aggregator::WeightedAverage { ..} => "weighted_average".into(),
            Aggregator::Moe { .. } => "MOE".into(),
            Aggregator::WeightedAverageMoe { .. } => "weighted_average_moe".into(),
            Aggregator::Custom(_) => "custom".into(),
        };

        AggregatorMetadata {
            name,
        }
    }
}
