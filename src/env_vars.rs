#[derive(Debug, Clone)]
pub struct EnvVars {
    pub secret: Option<String>,
}

impl EnvVars {
    pub fn new() -> Self {
        EnvVars {
            secret: None,
        }
    }
}
