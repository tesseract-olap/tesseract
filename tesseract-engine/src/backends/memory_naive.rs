use csv;
use failure::Error;
use indexmap::IndexMap;

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
}

impl Table {
    pub fn create(table_schema: &str) -> Result<Self, Error> {
        let mut table = Table {
            name: "".to_owned(),
            dim_cols_int: indexmap!{},
            mea_cols_int: indexmap!{},
            mea_cols_flt: indexmap!{},
            mea_cols_str: indexmap!{},
        };

        let mut lines = table_schema.lines();

        let table_name = lines.next()
            .ok_or(format_err!("No name for table found"))?
            .to_owned();

        table.name = table_name;

        let mut col_names = indexset!{};

        for line in table_schema.lines() {
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
    pub fn import_csv(&mut self, name: String, filepath: &str) -> Result<(), Error> {
        let mut rdr = csv::Reader::from_path(filepath)?;
        let header: Vec<String> = rdr.headers()?.into_iter()
            .map(|s| s.to_owned())
            .collect();

        for result in rdr.records() {
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

        Ok(())
    }
}

