/// tesseract-core contains Schema;
/// Schema is stateless; it is constructed from the schema file.
/// Schema is held in the AppState struct to provide access from a route
///
/// Each route instance will apply a tesseract_core::Query to tesseract_core::Schema to get sql.
/// The route instance then sends sql to database and gets results back in a
/// tesseract_core::Dataframe
///
/// Dataframe is then applied to Schema to format result. (for now, jsonrecords only)
///
///
/// Backend trait: exec() takes in a sql string, outputs a dataframe.

use failure::Error;

fn main() -> Result<(), Error> {
    println!("hello tesseract");
    Ok(())
}

#[derive(Debug, Clone)]
struct EnvVars {
    pub flush_secret: Option<String>,
    pub database_url: String,
    pub schema_filepath: Option<String>,
}

