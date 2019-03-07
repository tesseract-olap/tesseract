use itertools::join;

use crate::Aggregator;
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
    // hack for now... remove later
    fn agg_sql_string(m: &MeasureSql) -> String {
        match &m.aggregator {
            Aggregator::Sum => format!("sum({})", &m.column),
            Aggregator::Count => format!("count({})", &m.column),
            Aggregator::Average => format!("avg({})", &m.column),
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
    let mea_cols = join(meas.iter().map(|m| agg_sql_string(m)), ", ");

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

#[cfg(test)]
mod test {
    #[test]
    /// Tests:
    /// - basic standard sql generation
    /// - join dim table or inline
    /// - cuts on multi-level dim
    /// - parents
    ///
    fn test_standard_sql() {
        //"select valid_projects.id, name, sum(commits) from project_facts inner join valid_projects on project_facts.project_id = valid_projects.id where valid_projects.id=442841 group by name;"
        let table = TableSql {
            name: "project_facts".into(),
            primary_key: Some("id".into()),
        };
        let cuts = vec![
            CutSql {
                foreign_key: "project_id".into(),
                primary_key: "id".into(),
                table: Table { name: "valid_projects".into(), schema: None, primary_key: None },
                column: "id".into(),
                members: vec!["3".into()],
                member_type: MemberType::NonText,
            },
        ];
        let drills = vec![
            // this dim is inline, so should use the fact table
            // also has parents, so has 
            DrilldownSql {
                alias_postfix: "".into(),
                foreign_key: "project_id".into(),
                primary_key: "id".into(),
                table: Table { name: "valid_projects".into(), schema: None, primary_key: None },
                level_columns: vec![
                    LevelColumn {
                        key_column: "id".into(),
                        name_column: Some("name".to_owned()),
                    },
                ],
                property_columns: vec![],
            },
        ];
        let meas = vec![
            MeasureSql { aggregator: Aggregator::Sum, column: "commits".into() }
        ];

        assert_eq!(
            standard_sql(&table, &cuts, &drills, &meas, &None, &None, &None, &None, &None),
            "select valid_projects.id, valid_projects.name, sum(commits) from project_facts inner join valid_projects on valid_projects.id = project_facts.project_id where valid_projects.id in (3) group by valid_projects.id, valid_projects.name;".to_owned()
        );
    }
}

