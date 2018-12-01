use serde_derive::{Serialize, Deserialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct FlushQueryOpt {
    pub secret: String,
}

