mod dataframe;
pub mod names;
mod schema;
mod schema_config;
mod sql;
mod query;

use failure::{Error, format_err};

pub use self::dataframe::{DataFrame, Column, ColumnData};
use self::names::{
    Cut,
    Drilldown,
    Measure,
};
pub use self::schema::{Schema, Cube};
use self::schema_config::SchemaConfig;
use self::sql::{
    CutSql,
    DrilldownSql,
    MeasureSql,
    MemberType,
    TableSql,
};
pub use self::query::Query;


impl Schema {
    pub fn from_json(raw_schema: &str) -> Result<Self, Error> {
        let schema_config = serde_json::from_str::<SchemaConfig>(raw_schema)?;

        Ok(schema_config.into())
    }

    pub fn cube_metadata(&self, cube_name: &str) -> Option<Cube> {
        // Takes the first cube with the name.
        // TODO we still have to check that the cube names are distinct
        // before this.
        self.cubes.iter().find(|c| c.name == cube_name).cloned()
    }

    pub fn sql_query(
        &self,
        cube: &str,
        query: &Query,
        db: Database
        ) -> Result<String, Error>
    {
        // First do checks, like making sure there's a measure, and that there's
        // either a cut or drilldown
        if query.measures.is_empty() {
            return Err(format_err!("No measure found; please specify at least one"));
        }
        if query.drilldowns.is_empty() && query.cuts.is_empty(){
            return Err(format_err!("Either a drilldown or cut is required"));
        }

        // now get the database metadata
        let table = self.cube_table(&cube)
            .ok_or(format_err!("No table found for cube {}", cube))?;

        let cut_cols = self.cube_cut_cols(&cube, &query.cuts)
            .map_err(|err| format_err!("Error getting cut cols: {}", err))?;

        let drill_cols = self.cube_drill_cols(&cube, &query.drilldowns)
            .map_err(|err| format_err!("Error getting drill cols: {}", err))?;

        let mea_cols = self.cube_mea_cols(&cube, &query.measures)
            .map_err(|err| format_err!("Error getting mea cols: {}", err))?;


        // now feed the database metadata into the sql generator
        match db {
            Database::Clickhouse => {
                Ok(sql::clickhouse_sql(
                    &table,
                    &cut_cols,
                    &drill_cols,
                    &mea_cols,
                ))
            }
        }
    }

    //pub fn post_calculations(cal: &Calculations, df: DataFrame) -> DataFrame {
    //}

    pub fn format_results(&self, _df: DataFrame) -> String {
        "".to_owned()
    }
}

impl Schema {
    fn cube_table(&self, cube_name: &str) -> Option<TableSql> {
        self.cubes.iter()
            .find(|cube| &cube.name == &cube_name)
            .map(|cube| {
                TableSql {
                    name: cube.name.clone(),
                    primary_key: cube.table.primary_key.clone(),
                }
            })
    }

    fn cube_cut_cols(&self, cube_name: &str, cuts: &[Cut]) -> Result<Vec<CutSql>, Error> {
        let cube = self.cubes.iter()
            .find(|cube| &cube.name == &cube_name)
            .ok_or(format_err!("Could not find cube"))?;

        let mut res = vec![];

        for cut in cuts {
            let dim = cube.dimensions.iter()
                .find(|dim| dim.name == cut.level_name.dimension)
                .ok_or(format_err!("could not find dimension for cut"))?;
            let hier = dim.hierarchies.iter()
                .find(|hier| hier.name == cut.level_name.hierarchy)
                .ok_or(format_err!("could not find hierarchy for cut"))?;
            let level = hier.levels.iter()
                .find(|lvl| lvl.name == cut.level_name.level)
                .ok_or(format_err!("could not find level for cut"))?;

            // table currently required in hierarchy
            let table = hier.table
                .clone()
                .ok_or(format_err!("table is currently required in hierarchy"))?;

            // primary key is currently required in hierarchy. because inline dim is not yet
            // allowed
            let primary_key = hier.primary_key.clone();

            let foreign_key = dim.foreign_key
                .clone()
                .ok_or(format_err!("No foreign key; it's required for now (until inline dim implemented)"))?;

            let column = level.key_column.clone();

            res.push(CutSql {
                table,
                primary_key,
                foreign_key,
                column,
                members: cut.members.clone(),
                member_type: dim.foreign_key_type.clone().unwrap_or(MemberType::NonText),
            });
        }

        Ok(res)
    }

    fn cube_drill_cols(&self, cube_name: &str, drills: &[Drilldown]) -> Result<Vec<DrilldownSql>, Error> {
        let cube = self.cubes.iter()
            .find(|cube| &cube.name == &cube_name)
            .ok_or(format_err!("Could not find cube"))?;

        let mut res = vec![];

        for drill in drills {
            let dim = cube.dimensions.iter()
                .find(|dim| dim.name == drill.0.dimension)
                .ok_or(format_err!("could not find dimension for drill"))?;
            let hier = dim.hierarchies.iter()
                .find(|hier| hier.name == drill.0.hierarchy)
                .ok_or(format_err!("could not find hierarchy for drill"))?;
            let level = hier.levels.iter()
                .find(|lvl| lvl.name == drill.0.level)
                .ok_or(format_err!("could not find hierarchy for drill"))?;

            // table currently required in hierarchy
            let table = hier.table
                .clone()
                .ok_or(format_err!("table is currently required in hierarchy"))?;

            // primary key is currently required in hierarchy. because inline dim is not yet
            // allowed
            let primary_key = hier.primary_key.clone();

            let foreign_key = dim.foreign_key
                .clone()
                .ok_or(format_err!("No foreign key; it's required for now (until inline dim implemented)"))?;

            let key_column = level.key_column.clone();
            let name_column = level.name_column.clone();

            res.push(DrilldownSql {
                table,
                primary_key,
                foreign_key,
                key_column,
                name_column,
            });
        }

        Ok(res)
    }
    fn cube_mea_cols(&self, cube_name: &str, meas: &[Measure]) -> Result<Vec<MeasureSql>, Error> {
        let cube = self.cubes.iter()
            .find(|cube| &cube.name == &cube_name)
            .ok_or(format_err!("Could not find cube"))?;

        let mut res = vec![];

        for measure in meas {
            let mea = cube.measures.iter()
                .find(|m| m.name == measure.0)
                .ok_or(format_err!("could not find dimension for drill"))?;

            res.push(MeasureSql {
                column: mea.column.clone(),
                aggregator: mea.aggregator.clone(),
            });
        }

        Ok(res)
    }
}

pub enum Database {
    Clickhouse,
}
