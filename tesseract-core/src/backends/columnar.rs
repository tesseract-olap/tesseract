use csv;
use failure::Error;
use indexmap::{IndexMap, IndexSet};

use query::BackendQuery;

#[derive(Debug)]
pub struct ColumnarTable {
    name: String,

    // an empty vec indicates that there's no
    // dictionary encoding
    dim_cols: IndexMap<String, Vec<usize>>,
    dim_dictionary_encodings: IndexMap<String, Vec<usize>>,

    mea_cols_int: IndexMap<String, Vec<isize>>,
    mea_cols_flt: IndexMap<String, Vec<f64>>,
    mea_cols_str: IndexMap<String, Vec<String>>,
    col_len: usize,
}

impl ColumnarTable {
    pub fn create(table_schema: &str) -> Result<Self, Error> {
        let mut table = ColumnarTable {
            name: "".to_owned(),
            dim_cols: indexmap!{},
            dim_dictionary_encodings: indexmap!{},
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
                    let col = table.dim_cols.entry((*name).to_owned()).or_insert(vec![]);
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
                if let Some(col) = self.dim_cols.get_mut(&header[i]) {
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

        self.dictionary_encode_dims();

        Ok(())
    }

    fn dictionary_encode_dims(&mut self) {
        for (col_name, mut dim_col) in self.dim_cols.iter_mut() {
            // first create dimension members set
            let mut dim_member_set = indexset!{};
            for member in dim_col.iter() {
                dim_member_set.insert(member.clone());
            }
            //println!("{:?}", dim_member_set);

            // Then map (reverse map) from value to index in the dim_col
            *dim_col = dim_col.into_iter()
                .map(|member| {
                    if let Some((i, _)) = dim_member_set.get_full(member) {
                        i
                    } else {
                        // this would be a logic bug, since the member set
                        // was just constructed from the dim col
                        panic!("logic bug");
                    }
                }).collect();

            // finally create a map from the index to the value
            // as an indexable vector
            self.dim_dictionary_encodings.insert(
                col_name.clone(),
                dim_member_set.into_iter().collect::<Vec<_>>(),
            );
        }
    }

    pub fn execute_query(&self, query: &BackendQuery) -> Result<String, Error> {
        // gather all cols in drilldowns and cuts

        let dim_cols: Vec<_> = self.dim_cols.iter()
            .filter_map(|(col_name, col)| {
                if query.drilldowns.contains(col_name) {
                    Some(col)
                } else {
                    None
                }
            })
            .collect();
        // println!("{:?}", dim_cols);
        let dim_encodings: Vec<_> = self.dim_dictionary_encodings.iter()
            .filter_map(|(col_name, col)| {
                if query.drilldowns.contains(col_name) {
                    Some(col)
                } else {
                    None
                }
            })
            .collect();

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

        // create bit-packed "index"
        let dim_col_widths: Vec<usize> = dim_cols.iter()
            .map(|col| (*col.iter().max().unwrap() as f64).log2().floor() as usize + 1)
            .collect();

        println!("{:?}", dim_col_widths);

        let mut dim_index: Vec<usize> = Vec::new();
        for i in 0..self.col_len {
            let mut dim_cols = dim_cols.iter();
            let mut dim_packed_idx = dim_cols.next().unwrap()[i];

            for (dim_col, shift) in dim_cols.zip(&dim_col_widths) {
                ////println!("{}, {}, {}", dim_packed_idx, dim_col[i], shift);
                dim_packed_idx += dim_col[i] << shift;
                //println!("{}", dim_packed_idx);
            }
            dim_index.push(dim_packed_idx);
        }

        // This is used to construct the max size of the aggregate
        // buffer
        // If no max, return error
        let max_group_idx = dim_index.iter()
            .max()
            .ok_or(format_err!("no max?"))?;

        let mut agg_state = AggState::new();

        // guts
        for col in mea_cols_int {
            let mut cnt_buffer = vec![0; max_group_idx + 1];
            let mut agg_buffer = vec![0; max_group_idx + 1];
            for (i,j) in dim_index.iter().enumerate() {
                agg_buffer[*j] += col[i];
                cnt_buffer[*j] += 1;
            }
            agg_state.mea_cols_int.push(agg_buffer);
            agg_state.cnt_cols_int.push(cnt_buffer);
        }

        for col in mea_cols_flt {
            let mut cnt_buffer = vec![0; max_group_idx + 1];
            let mut agg_buffer = vec![0.; max_group_idx + 1];
            for (i,j) in dim_index.iter().enumerate() {
                agg_buffer[*j] += col[i];
                cnt_buffer[*j] += 1;
            }
            agg_state.mea_cols_flt.push(agg_buffer);
            agg_state.cnt_cols_flt.push(cnt_buffer);
        }

        // store dims that had a count (intersected with measure)
        // Does not return null count dims
        // currently hardcoded to int col 0,
        // but should do an |
        let dims_materialized: Vec<_> = agg_state.cnt_cols_int[0].iter()
            .enumerate()
            .filter_map(|(i, n)| if *n > 0 { Some(i) } else { None } )
            .collect();

        // now materialize tuples and write to csv
        let mut wtr = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(vec![]);

        // unpack.
        let dim_col_masks: Vec<_> = dim_col_widths.iter()
            .map(|width| (0..*width).map(|x| 2usize.pow(x as u32)).sum::<usize>())
            .collect();

        //println!("{:?}", dim_col_widths);
        //println!("{:?}", dim_col_masks);

        for dim_packed_idx in dims_materialized {
            let mea_int = agg_state.mea_cols_int.iter()
                .map(|col| col[dim_packed_idx])
                .collect();
            let mea_flt = agg_state.mea_cols_flt.iter()
                .map(|col| col[dim_packed_idx])
                .collect();
            let mea_str = agg_state.mea_cols_str.iter()
                .map(|col| col[dim_packed_idx].to_owned())
                .collect();

            let mut dim_index = Vec::new();
            let mut shift = 0;
            for (width, mask) in dim_col_widths.iter().zip(&dim_col_masks) {
                dim_index.push((dim_packed_idx >> shift) & mask);
                shift = *width;
            }
            //println!("{:?}, {:?}", dim_packed_idx, dim_index);

            // hack, unencode here. Performance hit if there's lots of rows
            for (i, mut encoded_member) in dim_index.iter_mut().enumerate() {
                if dim_encodings[i].len() > 0 {
                    *encoded_member = dim_encodings[i][*encoded_member];
                }
            }

            let row = CsvRow {
                dim_index,
                mea_int,
                mea_flt,
                mea_str,
            };

            wtr.serialize(row)?;
        }
        let res = String::from_utf8(wtr.into_inner()?)?;

        Ok(res)
    }
}

#[derive(Debug, Serialize)]
struct CsvRow {
    dim_index: Vec<usize>,
    mea_int: Vec<isize>,
    mea_flt: Vec<f64>,
    mea_str: Vec<String>,
}


#[derive(Debug, Serialize)]
struct AggState {
    // hack for counting, should be one per agg col
    pub mea_cols_int: Vec<Vec<isize>>,
    pub mea_cols_flt: Vec<Vec<f64>>,
    pub mea_cols_str: Vec<Vec<String>>,
    pub cnt_cols_int: Vec<Vec<usize>>,
    pub cnt_cols_flt: Vec<Vec<usize>>,
    pub cnt_cols_str: Vec<Vec<usize>>,
}

impl AggState {
    // this initialization is for sum!
    pub fn new() -> Self {
        AggState {
            mea_cols_int: vec![],
            mea_cols_flt: vec![],
            mea_cols_str: vec![],
            cnt_cols_int: vec![],
            cnt_cols_flt: vec![],
            cnt_cols_str: vec![],
        }
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
