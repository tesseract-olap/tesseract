#[derive(Debug, Deserialize)]
pub struct AggregateQuery {
    drilldowns: Option<Vec<String>>,
    cuts: Option<Vec<String>>,
    measures: Option<Vec<String>>,
    properties: Option<Vec<String>>,
    parents: Option<bool>,
    debug: Option<bool>,
//    distinct: Option<bool>,
//    nonempty: Option<bool>,
//    sparse: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct FlushQuery {
    pub secret: String,
}

