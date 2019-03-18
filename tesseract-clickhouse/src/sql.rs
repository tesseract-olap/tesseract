mod aggregator;
mod cuts;
mod growth;
mod options;
mod primary_agg;
mod rca;

use tesseract_core::names::Mask;
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
    ) -> String
{
    let (mut final_sql, mut final_drill_cols) = {
        if let Some(rca) = rca {
            rca::calculate(table, cuts, drills, meas, rca)
        } else {
            primary_agg(table, cuts, drills, meas)
        }
    };

    if let Some(growth) = growth {
        let (sql, drill_cols) = growth::calculate(final_sql, &final_drill_cols, meas.len(), growth);
        final_sql = sql;
        final_drill_cols = drill_cols;
    }

    final_sql = wrap_options(final_sql, &final_drill_cols, top, top_where, sort, limit, filters);

    final_sql
}

// TODO test having not cuts or drilldowns
#[cfg(test)]
mod test {
    use super::*;
    use tesseract_core::Table;
    use tesseract_core::query_ir::{LevelColumn, MemberType};
    use tesseract_core::schema::aggregator::Aggregator;

    #[test]
    /// Tests:
    /// - basic sql generation
    /// - join dim table or inline
    /// - first join dim that matches fact table primary key
    /// - cuts on multi-level dim
    /// - parents
    ///
    /// TODO:
    /// - unique
    fn test_clickhouse_sql() {
        let table = TableSql {
            name: "sales".into(),
            primary_key: Some("product_id".into()),
        };
        let cuts = vec![
            CutSql {
                foreign_key: "product_id".into(),
                primary_key: "product_id".into(),
                table: Table { name: "dim_products".into(), schema: None, primary_key: None },
                column: "product_group_id".into(),
                members: vec!["3".into()],
                member_type: MemberType::NonText,
                mask: Mask::Include,
                for_match: false,
            },
        ];
        let drills = vec![
            // this dim is inline, so should use the fact table
            // also has parents, so has 
            DrilldownSql {
                alias_postfix: "".into(),
                foreign_key: "date_id".into(),
                primary_key: "date_id".into(),
                table: Table { name: "sales".into(), schema: None, primary_key: None },
                level_columns: vec![
                    LevelColumn {
                        key_column: "year".into(),
                        name_column: None,
                    },
                    LevelColumn {
                        key_column: "month".into(),
                        name_column: None,
                    },
                    LevelColumn {
                        key_column: "day".into(),
                        name_column: None,
                    },
                ],
                property_columns: vec![],
            },
            // this comes second, but should join first because of primary key match
            // on fact table
            DrilldownSql {
                alias_postfix: "".into(),
                foreign_key: "product_id".into(),
                primary_key: "product_id".into(),
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
                property_columns: vec![],
            },
        ];

        let meas = vec![
            MeasureSql { aggregator: Aggregator::Sum, column: "quantity".into() }
        ];

        let filters = vec![];

        assert_eq!(
            clickhouse_sql(&table, &cuts, &drills, &meas, &filters, &None, &None, &None, &None, &None, &None),
            "select * from (select year, month, day, product_group_id, product_group_label, product_id_raw, product_label, sum(m0) as final_m0 from (select year, month, day, product_id, product_group_id, product_group_label, product_id_raw, product_label, m0 from (select product_group_id, product_group_label, product_id_raw, product_label, product_id as product_id from dim_products where product_group_id in (3)) all inner join (select year, month, day, product_id, sum(quantity) as m0 from sales where product_id in (select product_id from dim_products where product_group_id in (3)) group by year, month, day, product_id) using product_id) group by year, month, day, product_group_id, product_group_label, product_id_raw, product_label) order by year, month, day, product_group_id, product_group_label, product_id_raw, product_label asc ".to_owned()
        );
    }

    #[test]
    fn cutsql_membertype() {
        let cuts = vec![
            CutSql {
                foreign_key: "".into(),
                primary_key: "".into(),
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
    #[test]
    fn drilldown_with_properties() {
        let _drill = DrilldownSql {
            alias_postfix: "".into(),
            foreign_key: "product_id".into(),
            primary_key: "product_id".into(),
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

//TODO move this to core
//        assert_eq!(
//            drill.col_string(),
//            "product_group_id, product_group_label, product_id_raw, product_label, hexcode, form".to_owned(),
//        );
    }

    #[test]
    fn drilldown_with_properties_qual() {
        let drill = DrilldownSql {
            alias_postfix: "".into(),
            foreign_key: "product_id".into(),
            primary_key: "product_id".into(),
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
    // TODO test: drilldowns%5B%5D=Date.Year&measures%5B%5D=Quantity, which has only inline dim

