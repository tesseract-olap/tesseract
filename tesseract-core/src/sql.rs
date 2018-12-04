use itertools::join;
use serde_derive::{Deserialize, Serialize};

use crate::schema::Table;

/// Error checking is done before this point. This string formatter
/// accepts any input
pub fn clickhouse_sql(
    table: TableSql,
    mut cuts: Vec<CutSql>,
    mut drills: Vec<DrilldownSql>,
    meas: Vec<MeasureSql>,
    ) -> String
{
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

    let mut dim_subqueries = vec![];

    while let Some(drill) = drills.pop() {
        if let Some(idx) = cuts.iter().position(|c| c.table == drill.table) {
            let cut = cuts.swap_remove(idx);

            dim_subqueries.push(
                dim_subquery(Some(drill),Some(cut))
            );
        } else {
            dim_subqueries.push(
                dim_subquery(Some(drill), None)
            );
        }
    }

    for cut in cuts {
        dim_subqueries.push(
            dim_subquery(None, Some(cut))
        );
    }

    if let Some(primary_key) = table.primary_key {
        if let Some(idx) = dim_subqueries.iter().position(|d| d.foreign_key == primary_key) {
            dim_subqueries.swap(0, idx);
        }
    }

    // Now second half, feed DimSubquery into the multiple joins with fact table


    // Finally, wrap with final agg and result

    "".to_owned()
}

pub struct TableSql {
    pub name: String,
    pub primary_key: Option<String>,
}

pub struct DrilldownSql {
    pub table: Table,
    pub primary_key: String,
    pub foreign_key: String,
    pub key_column: String,
    pub name_column: Option<String>,
}

impl DrilldownSql {
    fn col_string(&self) -> String {
        if let Some(ref name_col) = self.name_column {
            format!("{}, {}", name_col, self.key_column)
        } else {
            self.key_column.clone()
        }
    }
}

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

pub struct MeasureSql {
    pub aggregator: String,
    pub column: String,
}

impl MeasureSql {
    fn agg_col_string(&self) -> String {
        format!("{}({})", self.aggregator, self.column)
    }
}

struct DimSubquery {
    sql: String,
    foreign_key: String,
}

/// Collects a drilldown and cut together to create a subquery for the dimension table
/// Does not check for matching name, because that had to have been done
/// before submitting to this fn.
fn dim_subquery(drill: Option<DrilldownSql>, cut: Option<CutSql>) -> DimSubquery {
    match drill {
        Some(drill) => {
            let mut sql = format!("select {} from {}",
                drill.col_string(),
                drill.table.full_name(),
            );
            if let Some(cut) = cut {
                sql.push_str(&format!(" where {} in ({})",
                    cut.column,
                    cut.members_string(),
                )[..]);
            }
            return DimSubquery {
                sql,
                foreign_key: drill.foreign_key,
            };
        },
        None => {
            if let Some(cut) = cut {
                let sql = format!("select {} from {} where {} in ({})",
                    cut.primary_key,
                    cut.table.full_name(),
                    cut.column,
                    cut.members_string(),
                );

                return DimSubquery {
                    sql,
                    foreign_key: cut.foreign_key,
                }
            }
        }
    }

    DimSubquery {
        sql: "".to_owned(),
        foreign_key: "".to_owned(),
    }
}

// TODO test having not cuts or drilldowns
#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_clickhouse_sql() {
        let table = "test_table";
        let cuts = vec![
            CutSql {
                column: "age".into(),
                members: vec!["3".into()],
                member_type: MemberType::NonText,
            },
        ];
        let drills = vec![
            DrilldownSql { column: "geo".into() },
            DrilldownSql { column: "age".into() },
        ];
        let meas = vec![
            MeasureSql { aggregator: "sum".into(), column: "quantity".into() }
        ];

        assert_eq!(
            clickhouse_sql(table, &cuts, &drills, &meas),
            "
            select geo_id, geo_label, product_id, product_label, sum(mea0)
            from
            (
                (
                    select geo_id, geo_label from dim_geo
                )
                all inner join
                (

                    (
                        select product_cat_id, product_cat_label, product_id from dim_product
                        where product_cat_id = 11
                    )
                    all inner join
                    (
                        select geo_id, product_id, sum(quantity) as mea0 from sales
                        group by geo, product_id)
                    ) using product_id
                ) using geo_id
            )
            group by geo_id, geo_label, product_id, product_label
            order by geo_id, geo_label, product_id, product_label asc
            ;".to_owned()
        );
    }

    #[test]
    fn cutsql_membertype() {
        let cuts = vec![
            CutSql {
                column: "geo".into(),
                members: vec!["1".into(), "2".into()],
                member_type: MemberType::Text,
            },
            CutSql {
                column: "age".into(),
                members: vec!["3".into()],
                member_type: MemberType::NonText,
            },
        ];

        assert_eq!(
            format!("{}", cuts[0]),
            "where geo in ('1', '2')",
        );
        assert_eq!(
            format!("{}", cuts[1]),
            "where age in (3)",
        );
    }
}
