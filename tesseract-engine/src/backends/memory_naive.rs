use csv;
use failure::Error;

// basic in-memory engine
// just stores vecs of strings
// It's slow, but doesn't matter
// This is just for testing out queries
// without having to generate sql
#[derive(Debug)]
pub struct MemoryEngine {
    tables: Vec<Table>,
}

impl MemoryEngine {
    pub fn new() -> Self {
        MemoryEngine {
            tables: vec![],
        }
    }
    pub fn add_table(&mut self, name: String, filepath: &str) -> Result<(), Error>  {
        let table = Table::from_csv(name, filepath)?;
        self.tables.push(table);

        Ok(())
    }
}

#[derive(Debug)]
pub struct Table {
    name: String,
    header: Vec<String>,
    columns: Vec<Vec<String>>,
}

impl Table {
    pub fn from_csv(name: String, filepath: &str) -> Result<Table, Error> {
        let mut rdr = csv::Reader::from_path(filepath)?;
        let header: Vec<String> = rdr.headers()?.into_iter()
            .map(|s| s.to_owned())
            .collect();

        let num_fields = header.len();

        let mut columns = vec![vec![];num_fields];
        for result in rdr.records() {
            let record = result?;
            for (i, field) in record.into_iter().enumerate() {
                columns[i].push(field.to_owned());
            }
        }

        Ok(Table {
            name,
            header,
            columns,
        })
    }
}

