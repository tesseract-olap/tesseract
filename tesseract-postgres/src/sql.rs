mod aggregator;
mod growth;
mod options;
mod primary_agg;

use tesseract_core::query_ir::{
    TableSql,
    CutSql,
    DrilldownSql,
    MeasureSql,
    TopSql,
    TopWhereSql,
    SortSql,
    LimitSql,
    RcaSql,
    GrowthSql,
    FilterSql,
    dim_subquery,
};
use self::options::wrap_options;
use self::primary_agg::primary_agg;
use itertools::join;

/// Error checking is done before this point. This string formatter
/// accepts any input
pub fn postgres_sql(
    table: &TableSql,
    cuts: &[CutSql],
    drills: &[DrilldownSql],
    meas: &[MeasureSql],
    filters: &[FilterSql],
    // TODO put Filters and Calculations into own structs
    top: &Option<TopSql>,
    top_where: &Option<TopWhereSql>,
    sort: &Option<SortSql>,
    limit: &Option<LimitSql>,
    rca: &Option<RcaSql>,
    growth: &Option<GrowthSql>,
) -> String
{
    let mut final_sql = primary_agg(table, cuts, drills, meas);

    if let Some(growth) = growth {
        let (sql, drill_cols) = growth::calculate(final_sql, drills, meas.len(), growth);
        final_sql = sql;
    }


    // sorting magic
    let sort_alias: Option<String> = match sort {
        Some(sort_obj) => {
            let position = meas.iter().position(|mea_obj| mea_obj.column == sort_obj.column).expect("Missing column position");
            Some(format!("m{}", position))
        },
        _ => None
    };
    final_sql = wrap_options(final_sql, drills, top, top_where, sort, sort_alias, limit, filters);

    final_sql
}
