use itertools::join;
//use super::Aggregator;
extern crate tesseract_core;
use tesseract_core::Aggregator;

use super::{
    TableSql,
    CutSql,
    DrilldownSql,
    MeasureSql,
    dim_subquery,
};

/// Error checking is done before this point. This string formatter
/// accepts any input
pub fn primary_agg(
    table: &TableSql,
    cuts: &[CutSql],
    drills: &[DrilldownSql],
    meas: &[MeasureSql],
) -> String
{
    // hack for now... remove later
    fn agg_sql_string(measure_idx: usize, m: &MeasureSql) -> String {
        match &m.aggregator {
            Aggregator::Sum => format!("sum({}) as m{}", &m.column, measure_idx),
            Aggregator::Count => format!("count({}) as m{}", &m.column, measure_idx),
            Aggregator::Average => format!("avg({}) as m{}", &m.column, measure_idx),
            // median doesn't work like this
            Aggregator::Median => format!("median"),
            Aggregator::WeightedAverage {..} => format!("avg"),
            Aggregator::Moe {..} => format!(""),
            Aggregator::WeightedAverageMoe {..} => format!(""),
            Aggregator::Custom(s) => format!("{}", s),
        }
    }

    // --------------------------------------------------
    // copied from primary_agg for clickhouse
    let ext_drills: Vec<_> = drills.iter()
        .filter(|d| d.table.name != table.name)
        .collect();

    let drill_cols_w_aliases = join(drills.iter().map(|d| d.col_alias_string2()), ", ");
    let drill_cols = join(drills.iter().map(|d| d.col_qual_string()), ", ");
    let mea_cols = join(meas.iter().enumerate().map(|(idx, m)| agg_sql_string(idx,m)), ", ");

    let mut final_sql = format!("select {}, {} from {}",
                                drill_cols_w_aliases,
                                mea_cols,
                                table.name,
    );

    // join external dims
    if !ext_drills.is_empty() {
        let join_ext_dim_clauses = join(ext_drills.iter()
                                            .map(|d| {
                                                format!("inner join {} on {}.{} = {}.{}",
                                                        d.table.full_name(),
                                                        d.table.full_name(),
                                                        d.primary_key,
                                                        table.name,
                                                        d.foreign_key,
                                                )
                                            }), " "); // note for postgres do not use commas!

        final_sql = format!("{} {}", final_sql, join_ext_dim_clauses);
    }

    if !cuts.is_empty() {
        let cut_clauses = join(cuts.iter().map(|c| format!("{} in ({})", c.col_qual_string(), c.members_string())), " and ");
        final_sql = format!("{} where {}", final_sql, cut_clauses);
    }

    final_sql = format!("{} group by {}", final_sql, drill_cols); // remove semicolon

    final_sql
}
