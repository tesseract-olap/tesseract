use itertools::join;
use std::fmt;

/// Error checking is done before this point. This string formatter
/// accepts any input
pub fn clickhouse_sql(
    table: &str,
    cuts: &[CutSql],
    drills: &[DrilldownSql],
    meas: &[MeasureSql],
    ) -> String
{
    let drilldown_num = drills.len();

    let drills = join(drills.iter().map(|d| d.to_string()), ", ");
    let cuts = join(cuts.iter().map(|c| c.to_string()), " and ");
    let meas = join(meas.iter().map(|m| m.to_string()), ", ");

    let mut res = format!("select {}, {} from {} where {}",
        drills,
        meas,
        table,
        cuts,
    );

    if drilldown_num != 0 {
        res.push_str(&format!(" group by {};", drills)[..]);
    } else {
        res.push_str(";");
    }

    res
}

pub struct DrilldownSql {
    pub column: String,
}

impl fmt::Display for DrilldownSql {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.column)
    }
}

// TODO What happens if the dim member is not a number?
// For now, i'll just assume that it's always a number.
pub struct CutSql {
    pub column: String,
    pub members: Vec<String>,
}

impl fmt::Display for CutSql {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let members = join(&self.members, ", ");
        write!(f, "{} in ({})", self.column, members)
    }
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_clickhouse_sql() {
        let table = "test_table";
        let cuts = vec![
            CutSql { column: "geo".into(), members: vec!["1".into(), "2".into()] },
            CutSql { column: "age".into(), members: vec!["3".into()] },
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
            "select geo, age, sum(quantity) from test_table where \
            geo in (1, 2) and age in (3) group by geo, age;".to_owned()
        );
    }
}
