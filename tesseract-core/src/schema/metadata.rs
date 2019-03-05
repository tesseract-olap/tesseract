use serde_derive::Serialize;
use std::collections::HashMap;
use std::convert::From;

use super::{
    Schema,
    Cube,
    Dimension,
    Hierarchy,
    Level,
    Measure,
    Property,
    Annotation,
    aggregator::Aggregator,
};

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SchemaMetadata {
    pub name: String,
    pub cubes: Vec<CubeMetadata>,
    pub annotations: Option<AnnotationMetadata>,
}

impl From<&Schema> for SchemaMetadata {
    fn from(schema: &Schema) -> Self {
        let annotations = schema.annotations.as_ref()
            .map(|anns| {
                anns.as_slice().into()
            });

        SchemaMetadata {
            name: schema.name.clone(),
            cubes: schema.cubes.iter().map(|c| c.into()).collect(),
            annotations,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CubeMetadata {
    pub name: String,
    pub dimensions: Vec<DimensionMetadata>,
    pub measures: Vec<MeasureMetadata>,
    pub annotations: Option<AnnotationMetadata>,
}

impl From<&Cube> for CubeMetadata {
    fn from(cube: &Cube) -> Self {
        let annotations = cube.annotations.as_ref()
            .map(|anns| {
                anns.as_slice().into()
            });

        CubeMetadata {
            name: cube.name.clone(),
            dimensions: cube.dimensions.iter().map(|d| d.into()).collect(),
            measures: cube.measures.iter().map(|m| m.into()).collect(),
            annotations,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DimensionMetadata {
    pub name: String,
    pub hierarchies: Vec<HierarchyMetadata>,
    pub annotations: Option<AnnotationMetadata>,
}

impl From<&Dimension> for DimensionMetadata {
    fn from(dimension: &Dimension) -> Self {
        let annotations = dimension.annotations.as_ref()
            .map(|anns| {
                anns.as_slice().into()
            });

        DimensionMetadata {
            name: dimension.name.clone(),
            hierarchies: dimension.hierarchies.iter().map(|h| h.into()).collect(),
            annotations,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HierarchyMetadata {
    pub name: String,
    pub levels: Vec<LevelMetadata>,
    pub annotations: Option<AnnotationMetadata>,
}

impl From<&Hierarchy> for HierarchyMetadata {
    fn from(hierarchy: &Hierarchy) -> Self {
        let annotations = hierarchy.annotations.as_ref()
            .map(|anns| {
                anns.as_slice().into()
            });

        HierarchyMetadata {
            name: hierarchy.name.clone(),
            levels: hierarchy.levels.iter().map(|l| l.into()).collect(),
            annotations,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LevelMetadata {
    pub name: String,
    pub properties: Option<Vec<PropertyMetadata>>,
    pub annotations: Option<AnnotationMetadata>,
}

impl From<&Level> for LevelMetadata {
    fn from(level: &Level) -> Self {
        let properties = level.properties.clone().map(|props| {
                props.iter().map(|p| p.into()).collect()
            });
        let annotations = level.annotations.as_ref()
            .map(|anns| {
                anns.as_slice().into()
            });

        LevelMetadata {
            name: level.name.clone(),
            properties,
            annotations,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MeasureMetadata {
    pub name: String,
    pub aggregator: AggregatorMetadata,
    pub annotations: Option<AnnotationMetadata>,
}

impl From<&Measure> for MeasureMetadata {
    fn from(measure: &Measure) -> Self {
        let annotations = measure.annotations.as_ref()
            .map(|anns| {
                anns.as_slice().into()
            });

        MeasureMetadata {
            name: measure.name.clone(),
            aggregator: (&measure.aggregator).into(),
            annotations,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PropertyMetadata {
    pub name: String,
    pub annotations: Option<AnnotationMetadata>,
}

impl From<&Property> for PropertyMetadata {
    fn from(property: &Property) -> Self {
        let annotations = property.annotations.as_ref()
            .map(|anns| {
                anns.as_slice().into()
            });

        PropertyMetadata {
            name: property.name.clone(),
            annotations,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AnnotationMetadata(HashMap<String, String>);

impl From<&[Annotation]> for AnnotationMetadata {
    fn from(annotations: &[Annotation]) -> Self {
        let res = annotations.iter()
            .map(|ann| (ann.name.to_owned(), ann.text.to_owned()) )
            .collect();

        AnnotationMetadata(res)
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
