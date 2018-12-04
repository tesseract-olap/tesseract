use itertools::join;
use serde_derive::{Deserialize, Serialize};
use std::fmt;

/// Error checking is done before this point. This string formatter
/// accepts any input
pub fn clickhouse_sql(
    table: &TableSql,
    cuts: &[CutSql],
    drills: &[DrilldownSql],
    meas: &[MeasureSql],
    ) -> String
{
    let drilldown_num = drills.len();
    let cuts_num = cuts.len();

    let drills = join(drills.iter().map(|d| d.col_string()), ", ");
    let cuts = join(cuts.iter().map(|c| c.to_string()), " and ");
    let meas = join(meas.iter().map(|m| m.to_string()), ", ");

    let mut res = format!("select {}, {} from {}",
        drills,
        meas,
        table.name,
    );

    if cuts_num != 0 {
        res.push_str(&format!(" where {}", cuts)[..]);
    }

    if drilldown_num != 0 {
        res.push_str(&format!(" group by {}", drills)[..]);
    }

    res.push_str(";");

    res
}

pub struct TableSql {
    pub name: String,
    pub primary_key: Option<String>,
}

pub struct DrilldownSql {
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
    pub primary_key: String,
    pub foreign_key: String,
    pub column: String,
    pub members: Vec<String>,
    pub member_type: MemberType,
}

impl fmt::Display for CutSql {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let members = match self.member_type {
            MemberType::NonText => join(&self.members, ", "),
            MemberType::Text => {
                let quoted = self.members.iter()
                    .map(|m| format!("'{}'", m));
                join(quoted, ", ")
            }
        };
        write!(f, "{} in ({})", self.column, members)
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

impl fmt::Display for MeasureSql {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}({})", self.aggregator, self.column)
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
