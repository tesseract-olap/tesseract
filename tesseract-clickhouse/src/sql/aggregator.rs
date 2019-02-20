//! Applying aggregates to measures
//!
//! This is a little complex, because some formulas are complex.
//!
//! for performance reasons, there's an aggregation at the fact table scan level,
//! and then a second aggregation when rolling up to a parent level.
//!
//! This works great for simple aggregations like sum, doing it in two parts doesn't
//! affect the aggregation.
//!
//! However, for more complex formulas like median, weighted avg, and moe, applying the
//! full formula to the first pass loses information needed for the second pass.
//!
//! Therefore, I've hardcoded weighted avg and moe so that the sums are done in the first
//! pass, but then the formula is applied at the second pass.
//!
//! median is not yet implemented. Custom is halfway implemented, but will need some guardrails.

use log::*;
use itertools::join;
use tesseract_core::Aggregator;

/// First pass for aggregator
/// This is called only when doing aggregations on the fact table.
/// For more complex aggregations like weighted average and moe, some component
/// parts are aggregated here, but the equation (with divisions or other complex
/// arithmetic) are not called until the final pass
pub fn agg_sql_string_pass_1(col: &str, aggregator: &Aggregator, mea_idx: usize) -> String {
    info!("{:?}", aggregator);

    match aggregator {
        Aggregator::Sum => format!("sum({}) as m{}", col, mea_idx),
        Aggregator::Average => format!("avg({}) as m{}", col, mea_idx),
        Aggregator::Median => format!("median({}) as m{}", col, mea_idx),
        Aggregator::WeightedAverage { weight_column } => {
            format!("sum({0} * {1}) as m{2}_weighted_avg_num, sum({1}) as m{2}_weighted_avg_denom",
                col,
                weight_column,
                mea_idx,
            )
        },
        Aggregator::Moe { secondary_columns }=> {
            let secondaries = secondary_columns.iter().enumerate()
                .map(|(n, col)| {
                    format!("sum({}) as m{}_moe_secondary_{}", col, mea_idx, n)
                });

            format!("sum({}) as m{}_moe_primary, {}",
                col,
                mea_idx,
                join(secondaries, ", "),
            )
        },
        Aggregator::Custom(s) => {
            let custom = s.replace("{}", col);
            format!("{} as m{}", custom, mea_idx)
        },
    }
}

// this is used to select mea cols as they bubble up from the fact subquery through
// each subquery join
pub fn agg_sql_string_select_mea(aggregator: &Aggregator, mea_idx: usize) -> String {
    match aggregator {
        Aggregator::Sum => format!("m{0}", mea_idx),
        Aggregator::Average => format!("m{0}", mea_idx),
        Aggregator::Median => format!("m{0}", mea_idx),
        Aggregator::WeightedAverage { .. } => {
            format!("m{0}_weighted_avg_num, m{0}_weighted_avg_denom",
                mea_idx,
            )
        },
        Aggregator::Moe { secondary_columns }=> {
            let secondaries = secondary_columns.iter().enumerate()
                .map(|(n, _)| {
                    format!("m{}_moe_secondary_{}", mea_idx, n)
                });

            format!("m{}_moe_primary, {}",
                mea_idx,
                join(secondaries, ", "),
            )
        },
        Aggregator::Custom(_) => format!("m{}", mea_idx),
    }
}

/// computes final formula for aggregates after all joins
/// For simple aggregates, can just apply the fn and add alias
///
/// For more complex aggregations, the full formula an be applied at this level
pub fn agg_sql_string_pass_2(aggregator: &Aggregator, mea_idx: usize) -> String {
    info!("{:?}", aggregator);

    match aggregator {
        Aggregator::Sum => format!("sum(m{0}) as final_m{0}", mea_idx),
        Aggregator::Average => format!("avg(m{0}) as final_m{0}", mea_idx),
        Aggregator::Median => format!("median(m{0}) as final_m{0}", mea_idx),
        Aggregator::WeightedAverage { .. } => {
            format!("(sum(m{0}_weighted_avg_num) / sum(m{0}_weighted_avg_denom)) as final_m{0}",
                mea_idx,
            )
        },
        Aggregator::Moe { secondary_columns }=> {
            let inner_seq = secondary_columns.iter().enumerate()
                .map(|(n, _)| {
                    format!("pow(sum(m{0}_moe_primary) - sum(m{0}_moe_secondary_{1}), 2)",
                        mea_idx,
                        n,
                    )
                });
            let inner_seq = join(inner_seq, " + ");

            format!("1.645 * sqrt(0.05 * ({}))",
                inner_seq,
            )
        },
        Aggregator::Custom(s) => {
            let custom = s.replace("{}", &format!("m{}", mea_idx));
            format!("{} as m{}", custom, mea_idx)
        },
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use tesseract_core::schema::aggregator::Aggregator;

    #[test]
    fn basic_aggs() {
        assert_eq!(
            agg_sql_string_pass_1("col_1".into(), &Aggregator::Sum, 0),
            "sum(col_1) as m0".to_owned(),
        );
        assert_eq!(
            agg_sql_string_pass_2(&Aggregator::Sum, 0),
            "sum(m0) as final_m0".to_owned(),
        );
        assert_eq!(
            agg_sql_string_select_mea(&Aggregator::Sum, 0),
            "m0".to_owned(),
        );
    }

    #[test]
    fn weighted_avg() {
        let agg = Aggregator::WeightedAverage {
            weight_column: "weight_col".into(),
        };
        assert_eq!(
            agg_sql_string_pass_1("col_1".into(), &agg, 0),
            "sum(col_1 * weight_col) as m0_weighted_avg_num, sum(weight_col) as m0_weighted_avg_denom".to_owned(),
        );
        assert_eq!(
            agg_sql_string_pass_2(&agg, 0),
            "(sum(m0_weighted_avg_num) / sum(m0_weighted_avg_denom)) as final_m0".to_owned(),
        );
        assert_eq!(
            agg_sql_string_select_mea(&agg, 0),
            "m0_weighted_avg_num, m0_weighted_avg_denom".to_owned(),
        );
    }

    #[test]
    fn moe() {
        let agg = Aggregator::Moe {
            secondary_columns: vec!["s0".into(), "s1".into(), "s2".into()],
        };
        assert_eq!(
            agg_sql_string_pass_1("col_1".into(), &agg, 0),
            "sum(col_1) as m0_moe_primary, \
                sum(s0) as m0_moe_secondary_0, \
                sum(s1) as m0_moe_secondary_1, \
                sum(s2) as m0_moe_secondary_2\
            ".to_owned(),
        );
        assert_eq!(
            agg_sql_string_pass_2(&agg, 0),
            "1.645 * sqrt(0.05 * (\
                pow(sum(m0_moe_primary) - sum(m0_moe_secondary_0), 2) + \
                pow(sum(m0_moe_primary) - sum(m0_moe_secondary_1), 2) + \
                pow(sum(m0_moe_primary) - sum(m0_moe_secondary_2), 2)\
                ))\
            ".to_owned(),
        );
        assert_eq!(
            agg_sql_string_select_mea(&agg, 0),
            "m0_moe_primary, m0_moe_secondary_0, m0_moe_secondary_1, m0_moe_secondary_2".to_owned(),
        );
    }
}
