use failure::Error;
use indexmap::IndexMap;

#[derive(Debug, Clone)]
pub struct Query {
    pub drilldowns: Vec<String>,
//    pub cuts: Vec<String>,
    pub measures: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub dim_cols_int: IndexMap<String, Vec<usize>>,
    pub mea_cols_int: IndexMap<String, Vec<isize>>,
    pub mea_cols_flt: IndexMap<String, Vec<f64>>,
    pub mea_cols_str: IndexMap<String, Vec<String>>,
}
