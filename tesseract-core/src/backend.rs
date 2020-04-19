use failure::Error;
use futures::{Future, Stream};

use crate::dataframe::DataFrame;
use crate::query_ir::QueryIr;
use crate::sql;
use crate::schema::metadata::SchemaPhysicalData;

pub trait Backend {
    /// Takes in a fully-qualfiied path to a table name
    /// assumes table is in the structure of: id (integer), name (text), schema (json) 
    /// desired query output format.
    fn retrieve_schemas(&self, tablepath: &str, id: Option<&str>) -> Box<dyn Future<Item=Vec<SchemaPhysicalData>, Error=Error>> {
        unimplemented!()
    }

    /// Takes in a SQL string, outputs a DataFrame, which will go on to be formatted into the
    /// desired query output format.
    fn exec_sql(&self, sql: String) -> Box<dyn Future<Item=DataFrame, Error=Error>>;

    /// Takes in a SQL string, outputs a stream of
    /// DataFrames, which will go on to be formatted into the
    /// desired query output format.
    fn exec_sql_stream(&self, sql: String) -> Box<dyn Stream<Item=Result<DataFrame, Error>, Error=Error>> {
        unimplemented!()
    }

    fn box_clone(&self) -> Box<dyn Backend + Send + Sync>;

    /// Receives an intermediate representation of the Query
    /// (the table, col, and relationship info needed for each drill,
    /// mea, cut, etc.) and generates a `String` of sql. Cannot error,
    /// and all checks should be done before calling this.
    fn generate_sql(&self, query_ir: QueryIr) -> String {
        // standard sql implementation
        sql::standard_sql(
            &query_ir.table,
            &query_ir.cuts,
            &query_ir.drills,
            &query_ir.meas,
            &query_ir.top,
            &query_ir.sort,
            &query_ir.limit,
            &query_ir.rca,
            &query_ir.growth,
        )
    }

    /// This function allows administrators to update the content of a given schema.
    fn update_schema(&self, tablepath: &str, schema_name_id: &str, schema_content: &str) -> Box<dyn Future<Item=bool, Error=Error>> {
        unimplemented!()
    }

    fn delete_schema(&self, tablepath: &str, schema_name_id: &str) -> Box<dyn Future<Item=bool, Error=Error>> {
        unimplemented!()
    }

    fn add_schema(&self, tablepath: &str,  schema_name_id: &str, schema_content: &str) -> Box<dyn Future<Item=bool, Error=Error>> {
        unimplemented!()
    }
}

impl Clone for Box<dyn Backend + Send + Sync> {
    fn clone(&self) -> Box<dyn Backend + Send + Sync> {
        self.box_clone()
    }
}
