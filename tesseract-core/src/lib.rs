mod dataframe;
mod schema;
mod schema_config;
mod query;

use failure::Error;

pub use self::dataframe::{DataFrame, Column, ColumnData};
pub use self::schema::{Schema, Cube};
use self::schema_config::SchemaConfig;
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

    pub fn sql_query(&self, query: &Query, db: &Database) -> String {
        "".to_owned()
    }

    //pub fn post_calculations(cal: &Calculations, df: DataFrame) -> DataFrame {
    //}

    pub fn add_dim_metadata(&self, query: &Query, df: DataFrame) -> DataFrame {
        DataFrame::new()
    }

    pub fn format_results(&self, df: DataFrame) -> String {
        "".to_owned()
    }
}

pub enum Database {
    Clickhouse,
}
