#[derive(Debug, Clone)]
pub struct Query {
    pub drilldowns: Vec<String>,
//    pub cuts: Vec<String>,
    pub measures: Vec<String>,
}

