use serde_derive::Serialize;
use std::convert::Into;

use super::config::{
    LogicLayerConfig,
    AliasConfig,
    CubeAliasConfig,
    SharedDimensionAliasConfig,
    NamedSetsConfig,
    NamedSetConfig,
    LevelPropertyConfig,
};


#[derive(Debug, Clone, Serialize)]
pub struct LogicLayerConfigMeta {
    pub aliases: Option<AliasConfigMeta>,
    pub named_sets: Option<Vec<NamedSetsConfigMeta>>,
}

impl From<&LogicLayerConfig> for LogicLayerConfigMeta {
    fn from(ll_config: &LogicLayerConfig) -> Self {
        Self {
            aliases: ll_config.aliases.as_ref().map(Into::into),
            named_sets: ll_config.named_sets.as_ref().map(|v| v.iter().map(Into::into).collect()),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct AliasConfigMeta {
    pub cubes: Option<Vec<CubeAliasConfigMeta>>,
    pub shared_dimensions: Option<Vec<SharedDimensionAliasConfigMeta>>
}

impl From<&AliasConfig> for AliasConfigMeta {
    fn from(alias_config: &AliasConfig) -> Self {
        Self {
            cubes: alias_config.cubes.as_ref().map(|v| v.iter().map(Into::into).collect()),
            shared_dimensions: alias_config.shared_dimensions.as_ref().map(|v| v.iter().map(Into::into).collect()),
        }
    }
}

// TODO: Remove requirement for `alternatives`
#[derive(Debug, Clone, Serialize)]
pub struct CubeAliasConfigMeta {
    pub name: String,
    pub alternatives: Vec<String>,
    pub levels: Option<Vec<LevelPropertyConfigMeta>>,
    pub properties: Option<Vec<LevelPropertyConfigMeta>>
}

impl From<&CubeAliasConfig> for CubeAliasConfigMeta {
    fn from(cube_alias_config: &CubeAliasConfig) -> Self {
        Self {
            name: cube_alias_config.name.clone(),
            alternatives: cube_alias_config.alternatives.iter().map(Into::into).collect(),
            levels: cube_alias_config.levels.as_ref().map(|v| v.iter().map(Into::into).collect()),
            properties: cube_alias_config.properties.as_ref().map(|v| v.iter().map(Into::into).collect()),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SharedDimensionAliasConfigMeta {
    pub name: String,
    pub levels: Option<Vec<LevelPropertyConfigMeta>>,
    pub properties: Option<Vec<LevelPropertyConfigMeta>>
}

impl From<&SharedDimensionAliasConfig> for SharedDimensionAliasConfigMeta {
    fn from(shared_dimension_alias_config: &SharedDimensionAliasConfig) -> Self {
        Self {
            name: shared_dimension_alias_config.name.clone(),
            levels: shared_dimension_alias_config.levels.as_ref().map(|v| v.iter().map(Into::into).collect()),
            properties: shared_dimension_alias_config.properties.as_ref().map(|v| v.iter().map(Into::into).collect()),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct NamedSetsConfigMeta {
    pub level_name: String,
    pub sets: Vec<NamedSetConfigMeta>
}

impl From<&NamedSetsConfig> for NamedSetsConfigMeta {
    fn from(named_sets_config: &NamedSetsConfig) -> Self {
        Self {
            level_name: named_sets_config.level_name.clone(),
            sets: named_sets_config.sets.iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct NamedSetConfigMeta {
    pub set_name: String,
    pub values: Vec<String>
}

impl From<&NamedSetConfig> for NamedSetConfigMeta {
    fn from(named_set_config: &NamedSetConfig) -> Self {
        Self {
            set_name: named_set_config.set_name.clone(),
            values: named_set_config.values.iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LevelPropertyConfigMeta {
    pub current_name: String,
    pub unique_name: String
}

impl From<&LevelPropertyConfig> for LevelPropertyConfigMeta {
    fn from(level_property_config: &LevelPropertyConfig) -> Self {
        Self {
            current_name: level_property_config.current_name.clone(),
            unique_name: level_property_config.unique_name.clone(),
        }
    }
}
