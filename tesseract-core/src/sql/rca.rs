use crate::sql::primary_agg::primary_agg;
use super::{
    TableSql,
    CutSql,
    DrilldownSql,
    MeasureSql,
    RcaSql,
};

pub fn calculate(
    table: &TableSql,
    cuts: &[CutSql],
    drills: &[DrilldownSql],
    meas: &[MeasureSql],
    rca: &RcaSql,
    ) -> (String, String)
{
    primary_agg(table, cuts, drills, meas)
}
