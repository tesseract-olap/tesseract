use itertools::join;

use super::{
    TableSql,
    CutSql,
    DrilldownSql,
    MeasureSql,
    RateSql,
};


pub fn rate_calculation(
    table: &TableSql,
    cuts: &[CutSql],
    drills: &[DrilldownSql],
    meas: &[MeasureSql],
    rate: &RateSql,
    final_sql: &str,
    final_drill_cols: &str
) -> String
{
    let final_sql = final_sql.to_string().clone();
    let final_drill_cols = final_drill_cols.to_string().clone();

    let mut rate_sql = "(select ".to_string();
    let mut drill_aliases: Vec<String> = vec![];

    for drill in drills {
        let drill_alias = &drill.col_alias_only_vec()[0];

        drill_aliases.push(drill_alias.clone());

        rate_sql = format!("{}{} as {}, ",
                           rate_sql, drill.primary_key.clone(), drill_alias
        );
    }

    rate_sql = format!("{}{} as rate_col_id, {} as rate_mea from {}) as s1",
                       rate_sql, rate.column, meas[0].column, rate.table.name
    );

    rate_sql = format!("{} all inner join ({}) as s2", rate_sql, final_sql);

    rate_sql = format!("select {}, rate_col_id, rate_mea, final_m0 from {} using {}",
                       final_drill_cols,
                       rate_sql,
                       join(drill_aliases, ", ")
    );

    rate_sql = format!("select {}, final_m0, groupArray(rate_col_id) as rate_col_id, groupArray(rate_mea) as rate_mea from ({}) group by {}, final_m0",
                       final_drill_cols,
                       rate_sql,
                       final_drill_cols
    );

    rate_sql = format!("select {}, rate_col_id_final, rate_mea_final, final_m0 from ({}) array join rate_col_id as rate_col_id_final, rate_mea as rate_mea_final",
                       final_drill_cols, rate_sql
    );

    rate_sql = format!("select {}, final_m0, sum(rate_mea_final) / avg(final_m0) from ({}) where rate_col_id_final in ({}) group by {}, final_m0 order by {}",
                       final_drill_cols,
                       rate_sql,
                       join(rate.members.clone(), ", "),
                       final_drill_cols,
                       final_drill_cols
    );

    rate_sql
}
