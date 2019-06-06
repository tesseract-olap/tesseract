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
pub enum Aggregator {
    #[serde(rename="sum")]
    Sum,
    #[serde(rename="count")]
    Count,
    #[serde(rename="avg")]
    Average,
    /// Median
    ///
    /// Needs two steps. It's slow because there won't be aggregation on the first step, only
    /// median on the second
    #[serde(rename="basic_grouped_median")]
    BasicGroupedMedian{
        group_aggregator: String,
        group_dimension: String,
    },
    /// Weighted Sum is calculated against the measure's value column.
    /// sum(column * weight_column)
    ///
    /// First roll-up is sum(column * weight_column) as weighted_sum_first
    /// Second roll-up is sum(weighted_sum_first) as weighted_sum_final
    #[serde(rename="weighted_sum")]
    WeightedSum {
        weight_column: String,
    },
    /// Weighted Average is calculated against the measure's value column.
    /// sum(column * weight_column) / sum(weight_column)
    #[serde(rename="weighted_avg")]
    WeightedAverage {
        weight_column: String,
    },
    /// Where the measure column is the primary value,
    /// and a list of secondary column is provided to the MO aggregator:
    ///
    /// The general equation for Margin of Error is
    /// ```text
    /// 1.645 * pow(0.05 * (pow(sum(column) - sum(secondary_columns[0]), 2) + pow(sum(column) - sum(secondary_columns_[1]), 2) + ...), 0.5)
    /// ```
    #[serde(rename="moe")]
    Moe {
        design_factor: f64,
        secondary_columns: Vec<String>,
    },
    /// Where the measure column is the primary value,
    /// and a list of secondary weight columns is provided to the MO aggregator:
    ///
    /// The general equation for Margin of Error is
    /// ```text
    /// 1.645 * pow(0.05 * (pow(( sum(column * primary_weight)/sum(primary_weight) ) - ( sum(column * secondary_weight_columns[0])/sum(secondary_weight_columns[0]) ), 2) + pow(( sum(column * primary_weight)/sum(primary_weight) ) - ( sum(column * secondary_weight_columns[1]/sum(secondary_weight_columns[1]) ), 2) + ...), 0.5)
    /// ```
    #[serde(rename="weighted_average_moe")]
    WeightedAverageMoe {
        design_factor: f64,
        primary_weight: String,
        secondary_weight_columns: Vec<String>,
    },
    // This only works for straightforward aggregations, which will work across
    // two roll-ups. For example, median won't work across two roll-ups
    #[serde(rename="custom")]
    Custom(String),
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json;

    // temp struct for doing serde test
    #[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
    struct Measure {
        col: String,
        aggregator: Aggregator,
    }

    #[test]
    fn parse_basic() {
        let sum = r#"{ "col": "testcol", "aggregator": "sum" }"#;
        let parsed: Measure = serde_json::from_str(sum).unwrap();
        assert_eq!(parsed.aggregator, Aggregator::Sum);
    }

    #[test]
    #[ignore]
    fn parse_custom() {
        let sum = r#"{ "col": "testcol", "aggregator": { "custom": "{}*{}" } }"#;
        let parsed: Measure = serde_json::from_str(sum).unwrap();
        assert_eq!(parsed.aggregator, Aggregator::Custom("{}*{}".to_owned()));
    }
}
