mod aggregator;
mod cuts;
mod growth;
mod options;
mod primary_agg;
mod rate;
mod rca;

use tesseract_core::query_ir::{
    TableSql,
    CutSql,
    DrilldownSql,
    MeasureSql,
    HiddenDrilldownSql,
    RcaSql,
    GrowthSql,
    RateSql,
    dim_subquery,
};
use tesseract_core::{QueryIr};

use self::options::wrap_options;
use self::primary_agg::primary_agg;
use self::rate::rate_calculation;


/// Error checking is done before this point. This string formatter
/// accepts any input
pub fn clickhouse_sql(
    query_ir: &QueryIr
    ) -> String
{
    let meas = &query_ir.meas;

    let (mut final_sql, mut final_drill_cols) = {
        // HiddenDrilldownSql, for grouped median, only works with primar agg,
        // will currently silently fail if used for rca or rate. (you can see
        // here that it's simply not passed to calculations.
        let table = &query_ir.table;
        let cuts = &query_ir.cuts;
        let drills = &query_ir.drills;

        let rca = &query_ir.rca;
        let rate = &query_ir.rate;

        if let Some(rca) = rca {
            rca::calculate(table, cuts, drills, meas, rca)
        } else if let Some(rate) = rate {
            rate_calculation(table, cuts, drills, meas, rate)
        } else {
            primary_agg(table, cuts, drills, meas, Some(&query_ir.hidden_drills))
        }
    };
    if let Some(growth) = &query_ir.growth {
        let (sql, drill_cols) = growth::calculate(final_sql, &final_drill_cols, meas.len(), growth);
        final_sql = sql;
        final_drill_cols = drill_cols;
    }

    final_sql = wrap_options(final_sql, &final_drill_cols, &query_ir, meas.len());

    final_sql
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
