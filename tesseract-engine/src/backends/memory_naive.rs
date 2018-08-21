use csv;
use failure::Error;
use indexmap::IndexMap;
use xxhash2;

use query::BackendQuery;

// basic in-memory engine
// just stores vecs of strings
// It's slow, but doesn't matter
// This is just for testing out queries
// without having to generate sql
#[derive(Debug)]
pub struct MemoryNaive {
    tables: Vec<Table>,
}

impl MemoryNaive {
    pub fn new() -> Self {
        MemoryNaive {
            tables: vec![],
        }
    }
}

#[derive(Debug)]
pub struct Table {
    name: String,
    dim_cols_int: IndexMap<String, Vec<usize>>,
    mea_cols_int: IndexMap<String, Vec<isize>>,
    mea_cols_flt: IndexMap<String, Vec<f64>>,
    mea_cols_str: IndexMap<String, Vec<String>>,
    col_len: usize,
}

impl Table {
    pub fn create(table_schema: &str) -> Result<Self, Error> {
        let mut table = Table {
            name: "".to_owned(),
            dim_cols_int: indexmap!{},
            mea_cols_int: indexmap!{},
            mea_cols_flt: indexmap!{},
            mea_cols_str: indexmap!{},
            col_len: 0,
        };

        let mut lines = table_schema.lines();

        let table_name = lines.next()
            .ok_or(format_err!("No name for table found"))?
            .to_owned();

        table.name = table_name;

        let mut col_names = indexset!{};

        for line in lines {
            match line.split_whitespace().collect::<Vec<_>>().as_slice() {
                ["dim", name, _    ] => {
                    if !col_names.insert(name.to_owned()) {
                        bail!("{:?} already exists in table");
                    }
                    let col = table.dim_cols_int.entry((*name).to_owned()).or_insert(vec![]);
                },
                ["mea", name, "int"] => {
                    if !col_names.insert(name) {
                        bail!("{:?} already exists in table");
                    }
                    let col = table.mea_cols_int.entry((*name).to_owned()).or_insert(vec![]);
                },
                ["mea", name, "flt"] => {
                    if !col_names.insert(name) {
                        bail!("{:?} already exists in table");
                    }
                    let col = table.mea_cols_flt.entry((*name).to_owned()).or_insert(vec![]);
                },
                ["mea", name, "str"] => {
                    if !col_names.insert(name) {
                        bail!("{:?} already exists in table");
                    }
                    let col = table.mea_cols_str.entry((*name).to_owned()).or_insert(vec![]);
                },
                _ => bail!("Cannot parse line {:?}", line),
            }
        }

        Ok(table)
    }

    // Header with col names required.
    pub fn import_csv(&mut self, csv: &str) -> Result<(), Error> {
        let mut rdr = csv::Reader::from_reader(csv.as_bytes());
        let header: Vec<String> = rdr.headers()?.into_iter()
            .map(|s| s.to_owned())
            .collect();

        let mut records_len = 0;
        for result in rdr.records() {
            records_len += 1;
            let record = result?;
            for (i, field) in record.into_iter().enumerate() {
                // match col name and insert
                if let Some(col) = self.dim_cols_int.get_mut(&header[i]) {
                    col.push(field.parse()?);
                } else if let Some(col) = self.mea_cols_int.get_mut(&header[i]) {
                    col.push(field.parse()?);
                } else if let Some(col) = self.mea_cols_flt.get_mut(&header[i]) {
                    col.push(field.parse()?);
                } else if let Some(col) = self.mea_cols_str.get_mut(&header[i]) {
                    col.push(field.to_owned());
                } else {
                    bail!("No name found");
                }
            }
        }

        self.col_len = records_len;

        Ok(())
    }

    pub fn execute_query(&self, query: &BackendQuery) -> Result<String, Error> {
        // gather all cols in drilldowns and cuts

        let dim_cols: Vec<_> = self.dim_cols_int.iter()
            .filter_map(|(col_name, col)| {
                if query.drilldowns.contains(col_name) {
                    Some(col)
                } else {
                    None
                }
            })
            .collect();
        // println!("{:?}", dim_cols);

        let mea_cols_int: Vec<_> = self.mea_cols_int.iter()
            .filter_map(|(col_name, col)| {
                if query.measures.contains(col_name) {
                    Some(col)
                } else {
                    None
                }
            })
            .collect();

        let mea_cols_flt: Vec<_> = self.mea_cols_flt.iter()
            .filter_map(|(col_name, col)| {
                if query.measures.contains(col_name) {
                    Some(col)
                } else {
                    None
                }
            })
            .collect();

        let mea_cols_str: Vec<_> = self.mea_cols_str.iter()
            .filter_map(|(col_name, col)| {
                if query.measures.contains(col_name) {
                    Some(col)
                } else {
                    None
                }
            })
            .collect();

        let mut agg_state: IndexMap<u64, AggCols> = indexmap!{};

        // guts
        // change to fold?
        for i in 0..self.col_len {
            let dim_members: Vec<_> = dim_cols.iter().map(|col| col[i]).collect();
            let dim_hash = xxhash2::hash64(as_u8_slice(&dim_members), 0);

            let mea_cols_int_values = mea_cols_int.iter().map(|col| col[i]);
            let mea_cols_flt_values = mea_cols_flt.iter().map(|col| col[i]);
            let mea_cols_str_values = mea_cols_str.iter().map(|col| col[i].clone());

            let measures = agg_state.entry(dim_hash)
                .or_insert(AggCols::new(
                    dim_members,
                    mea_cols_int_values.len(),
                    mea_cols_flt_values.len(),
                    mea_cols_str_values.len(),
                ));


            // for now sum aggregation is hardcoded
            // lazy, str will just take last str value
            for (agg_value, row_value) in measures.mea_cols_int.iter_mut().zip(mea_cols_int_values) {
                *agg_value += row_value;
            }
            for (agg_value, row_value) in measures.mea_cols_flt.iter_mut().zip(mea_cols_flt_values) {
                *agg_value += row_value;
            }
            for (agg_value, row_value) in measures.mea_cols_str.iter_mut().zip(mea_cols_str_values) {
                *agg_value = row_value;
            }
        }

        // println!("{:?}", agg_state);

        let mut wtr = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(vec![]);
        for row in agg_state.values() {
            wtr.serialize(row)?;
        }
        let res = String::from_utf8(wtr.into_inner()?)?;

        Ok(res)
    }
}

#[derive(Debug, Serialize)]
struct CsvRow<'a> {
    dim_index: &'a[usize],
    measures: &'a AggCols,
}


#[derive(Debug, Serialize)]
struct AggCols {
    pub dim_members: Vec<usize>,
    pub mea_cols_int: Vec<isize>,
    pub mea_cols_flt: Vec<f64>,
    pub mea_cols_str: Vec<String>,
}

impl AggCols {
    // this initialization is for sum!
    pub fn new(
        dim_members: Vec<usize>,
        mea_cols_int_len: usize,
        mea_cols_flt_len: usize,
        mea_cols_str_len: usize,
    ) -> Self {
        AggCols {
            dim_members: dim_members,
            mea_cols_int: vec![0; mea_cols_int_len],
            mea_cols_flt: vec![0.; mea_cols_flt_len],
            mea_cols_str: vec!["".to_owned(); mea_cols_str_len],
        }
    }
}

fn as_u8_slice(v: &[usize]) -> &[u8] {
    unsafe {
        ::std::slice::from_raw_parts(
            v.as_ptr() as *const u8,
            v.len() * ::std::mem::size_of::<usize>(),
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_basic_backend() {
        let table_schema =
            "test_table\n\
            dim year int\n\
            dim dim_0 int\n\
            dim dim_1 int\n\
            mea mea_0 flt\n\
            mea mea_1 int\
            ";

        let test_csv =
            "year,dim_0,dim_1,mea_0,mea_1\n\
            2015,0,0,11.0,100\n\
            2015,0,1,12.0,200\n\
            2015,0,2,13.0,300\n\
            2015,1,0,14.0,400\n\
            2015,1,1,15.0,500\n\
            2015,1,2,16.0,600\n\
            2016,0,0,21.0,700\n\
            2016,0,1,22.0,800\n\
            2016,0,2,23.0,900\n\
            2016,1,0,24.0,1000\n\
            2016,1,1,25.0,1100\n\
            2016,1,2,26.0,1200\
            ";


        let mut table = Table::create(table_schema).unwrap();
        println!("{:?}", table);

        table.import_csv(test_csv).unwrap();
        println!("{:?}", table);

        let res = table.execute_query(&BackendQuery {
            drilldowns: vec!["dim_0".to_owned(), "dim_1".to_owned()],
            measures: vec!["mea_0".to_owned(), "mea_1".to_owned()],
        }).unwrap();

        println!("{}", res);

        panic!();
    }
}
