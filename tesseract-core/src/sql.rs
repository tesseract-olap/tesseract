use failure::Error;

pub fn clickhouse_sql(
    table: String,
    cuts: Vec<CutSql>,
    drills: Vec<DrilldownSql>,
    meas: Vec<MeasureSql>,
    ) -> Result<String, Error>
{
    Ok("".to_owned())
}

pub struct DrilldownSql {
    pub column: String,
}

pub struct CutSql {
    pub column: String,
    pub members: Vec<String>,
}

pub struct MeasureSql {
    pub aggregator: String,
    pub column: String,
}
