use itertools::join;
use serde_derive::{Deserialize, Serialize};

use crate::schema::Table;

/// Error checking is done before this point. This string formatter
/// accepts any input
pub fn clickhouse_sql(
    table: TableSql,
    cuts: &[CutSql],
    drills: &[DrilldownSql],
    meas: &[MeasureSql],
    ) -> String
{
    // Before first section, need to separate out inline dims.
    // These are the ones that have the same dim table as fact table.
    //
    // First section, get drill/cut combos lined up.
    //
    // First "zip" drill and cut into DimSubquery
    // - pop drill, attempt to match with cut (remove cut if used (sounds sketchy, but could swap
    // with empty struct))
    // - go through remaining cuts (if had swapped empty struct, go through ones that aren't empty)
    //
    // Then, the order is:
    // - any dimension that has the same primary key as the
    // - doesn't matter
    //
    // So just swap the primary key DimSubquery to the head

    let mut ext_drills: Vec<_> = drills.iter()
        .filter(|d| d.table.name != table.name)
        .collect();

    let mut ext_cuts: Vec<_> = cuts.iter()
        .filter(|c| c.table.name != table.name)
        .collect();

    let inline_drills: Vec<_> = drills.iter()
        .filter(|d| d.table.name == table.name)
        .collect();

    let inline_cuts: Vec<_> = cuts.iter()
        .filter(|c| c.table.name == table.name)
        .collect();

    let mut dim_subqueries = vec![];

    // external drill and cuts section

    while let Some(drill) = ext_drills.pop() {
        if let Some(idx) = ext_cuts.iter().position(|c| c.table == drill.table) {
            let cut = ext_cuts.swap_remove(idx);

            dim_subqueries.push(
                dim_subquery(Some(drill),Some(cut))
            );
        } else {
            dim_subqueries.push(
                dim_subquery(Some(drill), None)
            );
        }
    }

    for cut in ext_cuts {
        dim_subqueries.push(
            dim_subquery(None, Some(cut))
        );
    }

    if let Some(primary_key) = table.primary_key {
        if let Some(idx) = dim_subqueries.iter().position(|d| d.foreign_key == primary_key) {
            dim_subqueries.swap(0, idx);
        }
    }

    // Now set up table table query
    // Group by is hardcoded in because there's an assumption that at least one
    // dim exists
    //
    // This is also the section wher inline dims and cuts get put

    let mea_cols = meas
        .iter()
        .enumerate()
        .map(|(i, m)| format!("{} as m{}", m.agg_col_string(), i));
    let mea_cols = join(mea_cols, ", ");

    let inline_dim_cols = inline_drills.iter().map(|d| d.col_string());
    let dim_idx_cols = dim_subqueries.iter().map(|d| d.foreign_key.clone());
    let all_dim_cols = join(inline_dim_cols.chain(dim_idx_cols), ", ");

    let mut fact_sql = format!("select {}", all_dim_cols);

    fact_sql.push_str(
        &format!(", {} from {}", mea_cols, table.name)
    );

    if !inline_cuts.is_empty() {
        let inline_cut_clause = inline_cuts
            .iter()
            .map(|c| format!(" {} in ({})", c.column, c.members_string()));
        let inline_cut_clause = join(inline_cut_clause, "and ");

        fact_sql.push_str(
            &format!(" where {}", inline_cut_clause)
        );
    }

    fact_sql.push_str(
        &format!(" group by {}", all_dim_cols)
    );

    // Now second half, feed DimSubquery into the multiple joins with fact table
    // TODO allow for differently named cols to be joined on. (using an alias for as)

    let mut sub_queries = fact_sql;

    // initialize current dim cols with inline drills and idx cols (all dim cols)
    let mut current_dim_cols = vec![all_dim_cols];

    for dim_subquery in dim_subqueries {
        // This section needed to accumulate the dim cols that are being selected over
        // the recursive joins.
        if let Some(cols) = dim_subquery.dim_cols {
            current_dim_cols.push(cols);
        }

        let sub_queries_dim_cols = if !current_dim_cols.is_empty() {
            format!("{}, ", join(current_dim_cols.iter(), ", "))
        } else {
            "".to_owned()
        };

        // Now construct subquery
        sub_queries = format!("select {}{} from ({}) all inner join ({}) using {}",
            sub_queries_dim_cols,
            join((0..meas.len()).map(|i| format!("m{}", i)), ", "),
            dim_subquery.sql,
            sub_queries,
            dim_subquery.foreign_key
        );
    }

    // Finally, wrap with final agg and result
    let final_drill_cols = drills.iter().map(|drill| drill.col_string());
    let final_drill_cols = join(final_drill_cols, ", ");

    let final_mea_cols = meas.iter().enumerate().map(|(i, mea)| format!("{}(m{})", mea.aggregator, i));
    let final_mea_cols = join(final_mea_cols, ", ");

    format!("select {}, {} from ({}) group by {} order by {} asc;",
        final_drill_cols,
        final_mea_cols,
        sub_queries,
        final_drill_cols,
        final_drill_cols,
    )
}

#[derive(Debug, Clone)]
pub struct TableSql {
    pub name: String,
    pub primary_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DrilldownSql {
    pub table: Table,
    pub primary_key: String,
    pub foreign_key: String,
    pub level_columns: Vec<LevelColumn>,
}

impl DrilldownSql {
    fn col_string(&self) -> String {
        let cols = self.level_columns.iter()
            .map(|l| {
                if let Some(ref name_col) = l.name_column {
                    format!("{}, {}", name_col, l.key_column)
                } else {
                    l.key_column.clone()
                }
            });

        join(cols, ", ")
    }
}

// TODO make level column an enum, to deal better with
// levels with only key column and no name column?
#[derive(Debug, Clone)]
pub struct LevelColumn {
    pub key_column: String,
    pub name_column: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CutSql {
    pub table: Table,
    pub primary_key: String,
    pub foreign_key: String,
    pub column: String,
    pub members: Vec<String>,
    pub member_type: MemberType,
}

impl CutSql {
    fn members_string(&self) -> String {
        let members = match self.member_type {
            MemberType::NonText => join(&self.members, ", "),
            MemberType::Text => {
                let quoted = self.members.iter()
                    .map(|m| format!("'{}'", m));
                join(quoted, ", ")
            }
        };
        format!("{}", members)
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub enum MemberType {
    #[serde(rename="text")]
    Text,
    #[serde(rename="nontext")]
    NonText,
}

#[derive(Debug, Clone)]
pub struct MeasureSql {
    pub aggregator: String,
    pub column: String,
}

impl MeasureSql {
    fn agg_col_string(&self) -> String {
        format!("{}({})", self.aggregator, self.column)
    }
}

#[derive(Debug, Clone)]
struct DimSubquery {
    sql: String,
    foreign_key: String,
    dim_cols: Option<String>,
}

/// Collects a drilldown and cut together to create a subquery for the dimension table
/// Does not check for matching name, because that had to have been done
/// before submitting to this fn.
fn dim_subquery(drill: Option<&DrilldownSql>, cut: Option<&CutSql>) -> DimSubquery {
    match drill {
        Some(drill) => {
            let mut sql = format!("select {}, {} from {}",
                drill.col_string(),
                drill.primary_key.clone(),
                drill.table.full_name(),
            );
            if let Some(cut) = cut {
                sql.push_str(&format!(" where {} in ({})",
                    cut.column.clone(),
                    cut.members_string(),
                )[..]);
            }
            return DimSubquery {
                sql,
                foreign_key: drill.foreign_key.clone(),
                dim_cols: Some(drill.col_string()),
            };
        },
        None => {
            if let Some(cut) = cut {
                let sql = format!("select {} from {} where {} in ({})",
                    cut.primary_key.clone(),
                    cut.table.full_name(),
                    cut.column.clone(),
                    cut.members_string(),
                );

                return DimSubquery {
                    sql,
                    foreign_key: cut.foreign_key.clone(),
                    dim_cols: None,
                }
            }
        }
    }

    DimSubquery {
        sql: "".to_owned(),
        foreign_key: "".to_owned(),
        dim_cols: None,
    }
}

// TODO test having not cuts or drilldowns
#[cfg(test)]
mod test {
    use super::*;

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
            },
        ];
        let drills = vec![
            // this dim is inline, so should use the fact table
            // also has parents, so has 
            DrilldownSql {
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
            },
            // this comes second, but should join first because of primary key match
            // on fact table
            DrilldownSql {
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
            },
        ];
        let meas = vec![
            MeasureSql { aggregator: "sum".into(), column: "quantity".into() }
        ];

        assert_eq!(
            clickhouse_sql(table, &cuts, &drills, &meas),
            "".to_owned()
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
            },
            CutSql {
                foreign_key: "".into(),
                primary_key: "".into(),
                table: Table { name: "".into(), schema: None, primary_key: None },
                column: "age".into(),
                members: vec!["3".into()],
                member_type: MemberType::NonText,
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

    // TODO test: drilldowns%5B%5D=Date.Year&measures%5B%5D=Quantity, which has only inline dim
}
