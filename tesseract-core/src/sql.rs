mod growth;
mod options;
mod primary_agg;
mod rca;

use itertools::join;
use serde_derive::{Deserialize, Serialize};

use crate::schema::Table;
use crate::sql::{
    options::wrap_options,
    primary_agg::primary_agg,
};
use crate::query::{LimitQuery, SortDirection};


// Temporary, until we get an interface ready for generating different sql
// per backend.
pub enum SqlType {
    Clickhouse,
    Standard,
}

/// Error checking is done before this point. This string formatter
/// accepts any input
/// Currently just does the standard aggregation.
/// No calculations, primary aggregation is not split out.
pub fn standard_sql(
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
    let mea_cols = join(meas.iter().map(|m| m.agg_col_string()), ", ");

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

    if !cuts.is_empty() || !ext_drills.is_empty() {
        final_sql = format!("{} where", final_sql);
    }


    if !cuts.is_empty() {
        let cut_clauses = join(cuts.iter().map(|c| format!("{} in ({})", c.col_qual_string(), c.members_string())), ", ");
        final_sql = format!("{} {}", final_sql, cut_clauses);
    }

    final_sql = format!("{} group by {};", final_sql, drill_cols);

    final_sql
}

/// Error checking is done before this point. This string formatter
/// accepts any input
pub fn clickhouse_sql(
    table: &TableSql,
    cuts: &[CutSql],
    drills: &[DrilldownSql],
    meas: &[MeasureSql],
    // TODO put Filters and Calculations into own structs
    top: &Option<TopSql>,
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

    final_sql = wrap_options(final_sql, &final_drill_cols, top, sort, limit);

    final_sql
}

#[derive(Debug, Clone)]
pub struct TableSql {
    pub name: String,
    pub primary_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DrilldownSql {
    pub table: Table,
    pub primary_key: String,
    pub foreign_key: String,
    pub level_columns: Vec<LevelColumn>,
    pub property_columns: Vec<String>,
}

impl DrilldownSql {
    fn col_string(&self) -> String {
        let cols = self.col_vec();
        join(cols, ", ")
    }

    fn col_vec(&self) -> Vec<String> {
        let mut cols: Vec<_> = self.level_columns.iter()
            .map(|l| {
                if let Some(ref name_col) = l.name_column {
                    format!("{}, {}", l.key_column, name_col)
                } else {
                    l.key_column.clone()
                }
            }).collect();

        if self.property_columns.len() != 0 {
            cols.push(
                join(&self.property_columns, ", ")
            );
        }

        cols
    }

    fn col_qual_string(&self) -> String {
        let cols = self.col_qual_vec();
        join(cols, ", ")
    }

    fn col_qual_vec(&self) -> Vec<String> {
        let mut cols: Vec<_> = self.level_columns.iter()
            .map(|l| {
                if let Some(ref name_col) = l.name_column {
                    format!("{}.{}, {}.{}", self.table.name, l.key_column, self.table.name, name_col)
                } else {
                    format!("{}.{}", self.table.name, l.key_column)
                }
            }).collect();

        if self.property_columns.len() != 0 {
            let prop_cols_qual = self.property_columns.iter()
                .map(|p| {
                    format!("{}.{}", self.table.name, p)
                });

            cols.push(
                join(prop_cols_qual, ", ")
            );
        }

        cols
    }
}

// TODO make level column an enum, to deal better with
// levels with only key column and no name column?
#[derive(Debug, Clone, PartialEq)]
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

    fn col_qual_string(&self) -> String {
        format!("{}.{}", self.table.name, self.column)
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
pub struct TopSql {
    pub n: u64,
    pub by_column: String,
    pub sort_columns: Vec<String>,
    pub sort_direction: SortDirection,
}

#[derive(Debug, Clone)]
pub struct LimitSql {
    pub n: u64,
}

impl From<LimitQuery> for LimitSql {
    fn from(l: LimitQuery) -> Self {
        LimitSql {
            n: l.n,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SortSql {
    pub direction: SortDirection,
    pub column: String,
}

#[derive(Debug, Clone)]
pub struct RcaSql {
    // level col for dim 1
    pub drill_1: Vec<DrilldownSql>,
    // level col for dim 2
    pub drill_2: Vec<DrilldownSql>,
    pub mea: MeasureSql,
}

#[derive(Debug, Clone)]
pub struct GrowthSql {
    pub time_drill: DrilldownSql,
    pub mea: String,
}

#[derive(Debug, Clone)]
struct DimSubquery {
    sql: String,
    foreign_key: String,
    dim_cols: Option<String>,
}

// TODO can this be removed, and all cuts put into the fact table scan using `IN`?
/// Collects a drilldown and cut together to create a subquery for the dimension table
/// Does not check for matching name, because that had to have been done
/// before submitting to this fn.
fn dim_subquery(drill: Option<&DrilldownSql>, cut: Option<&CutSql>) -> DimSubquery {
    match drill {
        Some(drill) => {
            // TODO
            // - oops, primary key is mandatory in schema, if not in
            // schema-config, then it takes the lowest level's key_column
            // - make primary key optional and propagate.
            // if primary key exists
            // if primary key == lowest level col,
            // Or will just making an alias for the primary key work?
            // Then don't add primary key here.
            // Also, make primary key optional?
            let mut sql = format!("select {}, {} as {} from {}",
                drill.col_string(),
                drill.primary_key.clone(),
                drill.foreign_key.clone(),
                drill.table.full_name(),
            );
            // TODO can I delete this cut?
//            if let Some(cut) = cut {
//                sql.push_str(&format!(" where {} in ({})",
//                    cut.column.clone(),
//                    cut.members_string(),
//                )[..]);
//            }
            return DimSubquery {
                sql,
                foreign_key: drill.foreign_key.clone(),
                dim_cols: Some(drill.col_string()),
            };
        },
        // TODO remove this? This path should never be hit now.
        None => {
            if let Some(cut) = cut {
                let sql = format!("select {} as {} from {} where {} in ({})",
                    cut.primary_key.clone(),
                    cut.foreign_key.clone(),
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
                property_columns: vec![],
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
                property_columns: vec![],
            },
        ];
        let meas = vec![
            MeasureSql { aggregator: "sum".into(), column: "quantity".into() }
        ];

        assert_eq!(
            clickhouse_sql(&table, &cuts, &drills, &meas, &None, &None, &None, &None, &None),
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
    #[test]
    fn drilldown_with_properties() {
        let drill = DrilldownSql {
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
            drill.col_string(),
            "product_group_id, product_group_label, product_id_raw, product_label, hexcode, form".to_owned(),
        );
    }

    #[test]
    fn drilldown_with_properties_qual() {
        let drill = DrilldownSql {
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

    // TODO test: drilldowns%5B%5D=Date.Year&measures%5B%5D=Quantity, which has only inline dim

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
            MeasureSql { aggregator: "sum".into(), column: "commits".into() }
        ];

        assert_eq!(
            standard_sql(&table, &cuts, &drills, &meas, &None, &None, &None, &None, &None),
            "select valid_projects.id, valid_projects.name, sum(commits) from project_facts inner join valid_projects on valid_projects.id = project_facts.project_id where valid_projects.id in (3) group by valid_projects.id, valid_projects.name;".to_owned()
        );
    }
}
