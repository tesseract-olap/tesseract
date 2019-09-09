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
            aliases: ll_config.aliases.into(),
            named_sets: ll_config.named_sets.into(),
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
            cubes: alias_config.aliases.map(Into::into),
            shared_dimensions: alias_config.named_sets.map(Into::into),
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

#[derive(Debug, Clone, Serialize)]
pub struct SharedDimensionAliasConfigMeta {
    pub name: String,
    pub levels: Option<Vec<LevelPropertyConfigMeta>>,
    pub properties: Option<Vec<LevelPropertyConfigMeta>>
}

#[derive(Debug, Clone, Serialize)]
pub struct NamedSetsConfigMeta {
    pub level_name: String,
    pub sets: Vec<NamedSetConfigMeta>
}

#[derive(Debug, Clone, Serialize)]
pub struct NamedSetConfigMeta {
    pub set_name: String,
    pub values: Vec<String>
}

#[derive(Debug, Clone, Serialize)]
pub struct LevelPropertyConfigMeta {
    pub current_name: String,
    pub unique_name: String
}

