use failure::Error;
use indexmap::IndexMap;

#[derive(Debug, Clone)]
pub struct BackendQuery {
    pub drilldowns: Vec<String>,
//    pub cuts: Vec<String>,
    pub measures: Vec<String>,
}

