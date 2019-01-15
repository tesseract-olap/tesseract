use crate::sql::{
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

pub struct QueryIr {
    pub table: TableSql,
    pub cuts: Vec<CutSql>,
    pub drills: Vec<DrilldownSql>,
    pub meas: Vec<MeasureSql>,
    // TODO put Filters and Calculations into own structs
    pub top: Option<TopSql>,
    pub sort: Option<SortSql>,
    pub limit: Option<LimitSql>,
    pub rca: Option<RcaSql>,
    pub growth: Option<GrowthSql>,
}
