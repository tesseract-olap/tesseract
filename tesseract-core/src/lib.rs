mod dataframe;
mod schema;
mod schema_config;
mod query;

use failure::Error;

pub use self::dataframe::{DataFrame, Column};
pub use self::schema::Schema;
use self::schema_config::SchemaConfig;
pub use self::query::Query;


impl Schema {
    pub fn from_json(raw_schema: &str) -> Result<Self, Error> {
        let schema_config = serde_json::from_str::<SchemaConfig>(raw_schema)?;

        Ok(schema_config.into())
    }

    pub fn cubes_metadata(&self) -> Schema {
        self.clone()
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
