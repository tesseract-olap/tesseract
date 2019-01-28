use itertools::join;

use crate::query_ir::{
    TableSql,
    CutSql,
    DrilldownSql,
    MeasureSql,
    TopSql,
    SortSql,
    LimitSql,
    RcaSql,
    GrowthSql,
};

/// Error checking is done before this point. This string formatter
/// accepts any input
/// Currently just does the standard aggregation.
/// No calculations, primary aggregation is not split out.
pub(crate) fn standard_sql(
    table: &TableSql,
    cuts: &[CutSql],
    drills: &[DrilldownSql],
    meas: &[MeasureSql],
    // TODO put Filters and Calculations into own structs
    _top: &Option<TopSql>,
    _sort: &Option<SortSql>,
    _limit: &Option<LimitSql>,
    _rca: &Option<RcaSql>,
    _growth: &Option<GrowthSql>,
    ) -> String
{
    // --------------------------------------------------
    // copied from primary_agg for clickhouse
    let ext_drills: Vec<_> = drills.iter()
        .filter(|d| d.table.name != table.name)
        .collect();

    //let ext_cuts: Vec<_> = cuts.iter()
    //    .filter(|c| c.table.name != table.name)
    //    .collect();
    //let ext_cuts_for_inline = ext_cuts.clone();

    //let inline_drills: Vec<_> = drills.iter()
    //    .filter(|d| d.table.name == table.name)
    //    .collect();

    //let inline_cuts: Vec<_> = cuts.iter()
    //    .filter(|c| c.table.name == table.name)
    //    .collect();
    // --------------------------------------------------

    let drill_cols = join(drills.iter().map(|d| d.col_qual_string()), ", ");
    let mea_cols = join(meas.iter().map(|m| m.agg_col_string()), ", ");

    let mut final_sql = format!("select {}, {} from {}",
        drill_cols,
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
        }), ", ");

        final_sql = format!("{} {}", final_sql, join_ext_dim_clauses);
    }

    if !cuts.is_empty() {
        let cut_clauses = join(cuts.iter().map(|c| format!("{} in ({})", c.col_qual_string(), c.members_string())), ", ");
        final_sql = format!("{} WHERE {}", final_sql, cut_clauses);
    }

    final_sql = format!("{} group by {};", final_sql, drill_cols);
    final_sql
}
