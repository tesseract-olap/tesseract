#[derive(Debug, Clone, Deserialize)]
pub struct EnvVars {
    pub flush_secret: Option<String>,
    pub database: String,
    pub schema_filepath: Option<String>,
}

