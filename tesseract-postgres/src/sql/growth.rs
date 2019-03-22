use itertools::join;

use super::GrowthSql;

use tesseract_core::query_ir::{DrilldownSql};

pub fn calculate(
    final_sql: String,
    drills:  &[DrilldownSql],
    num_measures: usize,
    growth: &GrowthSql,
) -> (String, String)
{
    let final_drill_cols = drills.iter().map(|drill| drill.col_qual_string());
    let final_drill_cols = join(final_drill_cols, ", ");

    let non_time_drills: Vec<&DrilldownSql> = drills.iter().filter(|drill| drill.col_qual_string() != growth.time_drill.col_qual_string()).collect();

    // Get the column aliases for the non-time drilldowns
    // These columns will be used for partitioning the growth window function
    let final_non_time_drill_cols = non_time_drills.iter().map(|drill| {
        let tmp: Vec<String> = drill.col_alias_only_vec().iter().map(|alias| format!("subquery1.{}", alias)).collect();
        return join(tmp, ", ");
    });
    let final_non_time_drill_cols = join(final_non_time_drill_cols, ", ");

    let growth_sql = format!("SELECT *, \
        coalesce(m{measure_idx} - (lag(m{measure_idx}) OVER w), 0) as growth_value, \
        coalesce(((m{measure_idx} - (lag(m0) OVER w)) / (lag(m0::float) OVER w)), 0) as growth_pct \
        FROM ({0}) subquery1 \
        WINDOW w as (PARTITION BY {drilldowns_ex_time} ORDER BY subquery1.{time_col})",
                                 final_sql.to_string(),
                                 measure_idx=0,
                                 drilldowns_ex_time=final_non_time_drill_cols,
                                 time_col=growth.time_drill.col_alias_only_string());
    (growth_sql, "".to_string())
}
