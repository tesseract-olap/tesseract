use itertools::join;

use super::{
    TableSql,
    CutSql,
    DrilldownSql,
    MeasureSql,
    RateSql,
};

use crate::sql::primary_agg::primary_agg;

use tesseract_core::{Aggregator};


pub fn rate_calculation(
    table: &TableSql,
    cuts: &[CutSql],
    drills: &[DrilldownSql],
    meas: &[MeasureSql],
    rate: &RateSql
) -> (String, String)
{
    // Add a drilldown on the level we are getting the rate for
    let mut new_drills: Vec<DrilldownSql> = vec![];
    for drill in drills {
        new_drills.push(drill.clone());
    }
    new_drills.push(rate.drilldown_sql.clone());

    // Call primary agg
    let (mut final_sql, mut final_drill_cols) = {
        primary_agg(table, cuts, &new_drills, meas)
    };

    let mut rate_sql = "".to_string();

    // Wrap that around a pivot
    let original_drill_cols = drills.iter().map(|drill| drill.col_alias_only_string());
    let original_drill_cols = join(original_drill_cols, ", ");

    let rate_aggregator = match meas[0].aggregator {
        Aggregator::Count => "count".to_string(),
        _ => "sum".to_string()
    };

    rate_sql = format!("select {}, {}(final_m0) as final_m0_agg, groupArray(final_m0) as final_m0_rate",
        original_drill_cols, rate_aggregator
    );

    let rate_drill_cols = rate.drilldown_sql.col_alias_only_vec();
    for rate_drill_col in &rate_drill_cols {
        rate_sql = format!("{}, groupArray({}) as {}", rate_sql, rate_drill_col, rate_drill_col);
    }

    rate_sql = format!("{} from ({}) group by {}", rate_sql, final_sql, original_drill_cols);

    // Unpivot
    //    Watch out for the appropriate aggregation
    rate_sql = format!("select {}, final_m0_agg as final_m0, final_m0_rate from ({}) array join",
        final_drill_cols, rate_sql
    );

    for rate_drill_col in &rate_drill_cols {
        rate_sql = format!("{} {} as {},", rate_sql, rate_drill_col, rate_drill_col);
    }

    rate_sql = format!("{} final_m0_rate as final_m0_rate", rate_sql);

    // Final aggregation
    rate_sql = format!("select {}, final_m0, {}(final_m0_rate) / avg(final_m0) from ({}) where {} in ({}) group by {}, final_m0 order by {}",
        original_drill_cols,
        rate_aggregator,
        rate_sql,
        rate_drill_cols[0],
        join(rate.members.clone(), ", "),
        original_drill_cols,
        original_drill_cols
    );

    (rate_sql, original_drill_cols)
}
