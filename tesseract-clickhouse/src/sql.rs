use itertools::join;

mod aggregator;
mod cuts;
mod growth;
mod options;
mod primary_agg;
mod rca;

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
    RateSql,
    dim_subquery,
};
use self::options::wrap_options;
use self::primary_agg::primary_agg;


/// Error checking is done before this point. This string formatter
/// accepts any input
pub fn clickhouse_sql(
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
    rate: &Option<RateSql>,
    ) -> String
{
    let (mut final_sql, mut final_drill_cols) = {
        if let Some(rca) = rca {
            rca::calculate(table, cuts, drills, meas, rate, rca)
        } else {
            primary_agg(table, cuts, drills, meas, rate)
        }
    };

    if let Some(rate) = rate {
        final_sql = rate_calculation(
            table, cuts, drills, meas, rate, &final_sql, &final_drill_cols
        );
    }

    if let Some(growth) = growth {
        let (sql, drill_cols) = growth::calculate(final_sql, &final_drill_cols, meas.len(), growth);
        final_sql = sql;
        final_drill_cols = drill_cols;
    }

    final_sql = wrap_options(final_sql, &final_drill_cols, top, top_where, sort, limit, filters);

    final_sql
}


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



// TODO test having not cuts or drilldowns
#[cfg(test)]
mod test {
    use super::*;
    use tesseract_core::Table;
    use tesseract_core::names::Mask;
    use tesseract_core::query_ir::{LevelColumn, MemberType};

    // TODO move this to better place?
    // Should all of these internal checks be moved to one place? Is this an ok place?
    #[test]
    fn cutsql_membertype() {
        let cuts = vec![
            CutSql {
                foreign_key: "".into(),
                primary_key: "".into(),
                inline_table: None,
                table: Table { name: "".into(), schema: None, primary_key: None },
                column: "geo".into(),
                members: vec!["1".into(), "2".into()],
                member_type: MemberType::Text,
                mask: Mask::Include,
                for_match: false,
            },
            CutSql {
                foreign_key: "".into(),
                primary_key: "".into(),
                inline_table: None,
                table: Table { name: "".into(), schema: None, primary_key: None },
                column: "age".into(),
                members: vec!["3".into()],
                member_type: MemberType::NonText,
                mask: Mask::Include,
                for_match: false,
            },
        ];

        assert_eq!(
            cuts[0].members_string(),
            "'1', '2'",
        );
        assert_eq!(
            cuts[1].members_string(),
            "3",
        );
    }

    // TODO move this to better place?
    // Should all of these internal checks be moved to one place? Is this an ok place?
    #[test]
    fn drilldown_with_properties_qual() {
        let drill = DrilldownSql {
            alias_postfix: "".into(),
            foreign_key: "product_id".into(),
            primary_key: "product_id".into(),
            inline_table: None,
            table: Table { name: "dim_products".into(), schema: None, primary_key: None },
            level_columns: vec![
                LevelColumn {
                    key_column: "product_group_id".into(),
                    name_column: Some("product_group_label".into()),
                },
                LevelColumn {
                    key_column: "product_id_raw".into(),
                    name_column: Some("product_label".into()),
                },
            ],
            property_columns: vec!["hexcode".to_owned(), "form".to_owned()],
        };

        assert_eq!(
            drill.col_qual_string(),
            "dim_products.product_group_id, dim_products.product_group_label, dim_products.product_id_raw, dim_products.product_label, dim_products.hexcode, dim_products.form".to_owned(),
        );
    }

}

