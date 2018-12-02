// TODO switch to using name module
#[derive(Debug, Clone)]
pub struct Query {
    pub drilldowns: Vec<String>,
    pub cuts: Vec<String>,
    pub measures: Vec<String>,
}

impl Query {
    pub fn new() -> Self {
        Query {
            drilldowns: vec![],
            cuts: vec![],
            measures: vec![],
        }
    }
}
