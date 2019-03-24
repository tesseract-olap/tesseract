use itertools::join;

use super::GrowthSql;

use tesseract_core::query_ir::{DrilldownSql};

pub fn drilldown_list_helper(table_alias: &str, my_drills:  &Vec<&DrilldownSql>) -> String
{
    // This helper function takes a table alias and a vector of drilldowns
    // and returns a comma separated list of the column aliases prefixed by the table alias
    let time_drill_cols = my_drills.iter().map(|drill| {
        let tmp: Vec<String> = drill.col_alias_only_vec().iter().map(|alias| format!("{}.{}", table_alias, alias)).collect();
        return join(tmp, ", ");
    });
    let time_drill_cols = join(time_drill_cols, ", ");
    time_drill_cols
}

pub fn calculate(
    final_sql: String,
    drills:  &[DrilldownSql],
    num_measures: usize,
    growth: &GrowthSql,
) -> (String, String)
{
    let growth_table_alias = "growth_subquery";
    let non_time_drills: Vec<&DrilldownSql> = drills.iter().filter(|drill| drill.col_qual_string() != growth.time_drill.col_qual_string()).collect();
    let final_non_time_drill_cols: String = drilldown_list_helper(growth_table_alias, &non_time_drills);
    let growth_sql = format!("SELECT *, \
        coalesce(m{measure_idx} - (lag(m{measure_idx}) OVER w), 0) as growth_value, \
        coalesce(((m{measure_idx} - (lag(m0) OVER w)) / (lag(m0::float) OVER w)), 0) as growth_pct \
        FROM ({0}) {growth_table_alias} \
        WINDOW w as (PARTITION BY {drilldowns_ex_time} ORDER BY {growth_table_alias}.{time_col})",
                                 final_sql.to_string(),
                                 measure_idx=0,
                                 drilldowns_ex_time=final_non_time_drill_cols,
                                 growth_table_alias=growth_table_alias,
                                 time_col=growth.time_drill.col_alias_only_string());
    (growth_sql, "".to_string())
}
