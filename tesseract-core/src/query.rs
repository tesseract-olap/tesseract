// TODO switch to using name module

use crate::names::{
    Cut,
    Drilldown,
    Measure,
};

#[derive(Debug, Clone)]
pub struct Query {
    pub cuts: Vec<Cut>,
    pub drilldowns: Vec<Drilldown>,
    pub measures: Vec<Measure>,
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
