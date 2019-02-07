use failure::{Error, bail};
use serde_derive::{Deserialize, Serialize};

// TODO move this to a better place? Does this belong in query_ir?
// Median is the one that postgres and mysql don't support
// That means that the actual string generation happens
// inside each db's sql implementation
//
// For custom calculations,
// the col is referred to as {},
// and find and replace is used later to insert the col name
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Aggregator {
    #[serde(rename="sum")]
    Sum,
    #[serde(rename="avg")]
    Average,
    #[serde(rename="median")]
    Median,
    #[serde(rename="weighted-avg")]
    WeightedAverage,
    #[serde(rename="moe")]
    Moe,
    #[serde(rename="custom")]
    Custom(String),
}

