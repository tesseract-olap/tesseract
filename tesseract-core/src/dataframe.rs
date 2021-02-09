use failure::{format_err, Error};
use std::collections::HashMap;


#[derive(Debug)]
pub struct DataFrame {
    pub columns: Vec<Column>,
}

impl DataFrame {
    pub fn new() -> Self {
        DataFrame {
            columns: vec![],
        }
    }

    pub fn from_vec(columns: Vec<Column>) -> Self {
        DataFrame {
            columns
        }
    }

    pub fn len(&self) -> usize {
        match self.columns.get(0) {
            Some(col) => col.len(),
            None => 0,
        }
    }

    pub fn drain_filter<P>(mut self, predicate: P) -> Self
    where
        P: Fn(HashMap<&str, Datum>, usize) -> bool,
    {
        let col_amount = self.columns.len();

        let mut i = 0;
        while i < self.len() {
            let mut datum: HashMap<&str, Datum> = HashMap::with_capacity(col_amount);

            for column in &self.columns {
                let value: Datum = match &column.column_data {
                    ColumnData::Int8(d) => Datum::Int8(d[i]),
                    ColumnData::Int16(d) => Datum::Int16(d[i]),
                    ColumnData::Int32(d) => Datum::Int32(d[i]),
                    ColumnData::Int64(d) => Datum::Int64(d[i]),
                    ColumnData::UInt8(d) => Datum::UInt8(d[i]),
                    ColumnData::UInt16(d) => Datum::UInt16(d[i]),
                    ColumnData::UInt32(d) => Datum::UInt32(d[i]),
                    ColumnData::UInt64(d) => Datum::UInt64(d[i]),
                    ColumnData::Float32(d) => Datum::Float32(d[i]),
                    ColumnData::Float64(d) => Datum::Float64(d[i]),
                    ColumnData::Text(d) => Datum::Text(d[i].to_string()),
                    ColumnData::NullableInt8(d) => Datum::NullableInt8(d[i]),
                    ColumnData::NullableInt16(d) => Datum::NullableInt16(d[i]),
                    ColumnData::NullableInt32(d) => Datum::NullableInt32(d[i]),
                    ColumnData::NullableInt64(d) => Datum::NullableInt64(d[i]),
                    ColumnData::NullableUInt8(d) => Datum::NullableUInt8(d[i]),
                    ColumnData::NullableUInt16(d) => Datum::NullableUInt16(d[i]),
                    ColumnData::NullableUInt32(d) => Datum::NullableUInt32(d[i]),
                    ColumnData::NullableUInt64(d) => Datum::NullableUInt64(d[i]),
                    ColumnData::NullableFloat32(d) => Datum::NullableFloat32(d[i]),
                    ColumnData::NullableFloat64(d) => Datum::NullableFloat64(d[i]),
                    ColumnData::NullableText(d) => match &d[i] {
                        Some(nt) => Datum::NullableText(Some(nt.to_string())),
                        None => Datum::NullableText(None),
                    },
                };

                datum.insert(&column.name, value);
            }

            if !predicate(datum, i) {
                for column in self.columns.iter_mut() {
                    column.remove(i);
                }
            } else {
                i += 1;
            }
        }

        self
    }
}

#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub column_data: ColumnData,
}

impl Column {
    pub fn new(name: String, column_data: ColumnData) -> Self {
        Column {
            name,
            column_data,
        }
    }

    pub fn column_data(&mut self) ->&mut ColumnData {
        &mut self.column_data
    }

    pub fn len(&self) -> usize {
        match self.column_data {
            ColumnData::Int8(ref ns) => ns.len(),
            ColumnData::Int16(ref ns) => ns.len(),
            ColumnData::Int32(ref ns) => ns.len(),
            ColumnData::Int64(ref ns) => ns.len(),
            ColumnData::UInt8(ref ns) => ns.len(),
            ColumnData::UInt16(ref ns) => ns.len(),
            ColumnData::UInt32(ref ns) => ns.len(),
            ColumnData::UInt64(ref ns) => ns.len(),
            ColumnData::Float32(ref ns) => ns.len(),
            ColumnData::Float64(ref ns) => ns.len(),
            ColumnData::Text(ref ss) => ss.len(),
            ColumnData::NullableInt8(ref ns) => ns.len(),
            ColumnData::NullableInt16(ref ns) => ns.len(),
            ColumnData::NullableInt32(ref ns) => ns.len(),
            ColumnData::NullableInt64(ref ns) => ns.len(),
            ColumnData::NullableUInt8(ref ns) => ns.len(),
            ColumnData::NullableUInt16(ref ns) => ns.len(),
            ColumnData::NullableUInt32(ref ns) => ns.len(),
            ColumnData::NullableUInt64(ref ns) => ns.len(),
            ColumnData::NullableFloat32(ref ns) => ns.len(),
            ColumnData::NullableFloat64(ref ns) => ns.len(),
            ColumnData::NullableText(ref ss) => ss.len(),
        }
    }

    /// Sort column entries for all types, but floats.
    pub fn sort_column_data(&mut self) -> Result<(), Error> {
        match self.column_data {
            ColumnData::Int8(ref mut v) => v.sort(),
            ColumnData::Int16(ref mut v) => v.sort(),
            ColumnData::Int32(ref mut v) => v.sort(),
            ColumnData::Int64(ref mut v) => v.sort(),
            ColumnData::UInt8(ref mut v) => v.sort(),
            ColumnData::UInt16(ref mut v) => v.sort(),
            ColumnData::UInt32(ref mut v) => v.sort(),
            ColumnData::UInt64(ref mut v) => v.sort(),
            ColumnData::Float32(_) => {
                return Err(format_err!("Cannot sort Float32 column"));
            },
            ColumnData::Float64(_) => {
                return Err(format_err!("Cannot sort Float64 column"));
            },
            ColumnData::Text(ref mut v) => v.sort(),
            ColumnData::NullableInt8(ref mut v) => v.sort(),
            ColumnData::NullableInt16(ref mut v) => v.sort(),
            ColumnData::NullableInt32(ref mut v) => v.sort(),
            ColumnData::NullableInt64(ref mut v) => v.sort(),
            ColumnData::NullableUInt8(ref mut v) => v.sort(),
            ColumnData::NullableUInt16(ref mut v) => v.sort(),
            ColumnData::NullableUInt32(ref mut v) => v.sort(),
            ColumnData::NullableUInt64(ref mut v) => v.sort(),
            ColumnData::NullableFloat32(_) => {
                return Err(format_err!("Cannot sort NullableFloat32 column"));
            },
            ColumnData::NullableFloat64(_) => {
                return Err(format_err!("Cannot sort NullableFloat64 column"));
            },
            ColumnData::NullableText(ref mut v) => v.sort(),
        }

        Ok(())
    }

    /// DataFrame columns can come in many different types. This function converts
    /// all data to a common type (String).
    pub fn stringify_column_data(&self) -> Vec<String> {
        return match &self.column_data {
            ColumnData::Int8(v) => v.iter().map(|&e| e.to_string()).collect(),
            ColumnData::Int16(v) => v.iter().map(|&e| e.to_string()).collect(),
            ColumnData::Int32(v) => v.iter().map(|&e| e.to_string()).collect(),
            ColumnData::Int64(v) => v.iter().map(|&e| e.to_string()).collect(),
            ColumnData::UInt8(v) => v.iter().map(|&e| e.to_string()).collect(),
            ColumnData::UInt16(v) => v.iter().map(|&e| e.to_string()).collect(),
            ColumnData::UInt32(v) => v.iter().map(|&e| e.to_string()).collect(),
            ColumnData::UInt64(v) => v.iter().map(|&e| e.to_string()).collect(),
            ColumnData::Float32(v) => v.iter().map(|&e| e.to_string()).collect(),
            ColumnData::Float64(v) => v.iter().map(|&e| e.to_string()).collect(),
            ColumnData::Text(v) => v.to_vec(),
            ColumnData::NullableInt8(v) => {
                v.iter().map(|&e| {
                    match e {
                        Some(e) => e.to_string(),
                        None => "".to_string()
                    }
                }).collect()
            },
            ColumnData::NullableInt16(v) => {
                v.iter().map(|&e| {
                    match e {
                        Some(e) => e.to_string(),
                        None => "".to_string()
                    }
                }).collect()
            },
            ColumnData::NullableInt32(v) => {
                v.iter().map(|&e| {
                    match e {
                        Some(e) => e.to_string(),
                        None => "".to_string()
                    }
                }).collect()
            },
            ColumnData::NullableInt64(v) => {
                v.iter().map(|&e| {
                    match e {
                        Some(e) => e.to_string(),
                        None => "".to_string()
                    }
                }).collect()
            },
            ColumnData::NullableUInt8(v) => {
                v.iter().map(|&e| {
                    match e {
                        Some(e) => e.to_string(),
                        None => "".to_string()
                    }
                }).collect()
            },
            ColumnData::NullableUInt16(v) => {
                v.iter().map(|&e| {
                    match e {
                        Some(e) => e.to_string(),
                        None => "".to_string()
                    }
                }).collect()
            },
            ColumnData::NullableUInt32(v) => {
                v.iter().map(|&e| {
                    match e {
                        Some(e) => e.to_string(),
                        None => "".to_string()
                    }
                }).collect()
            },
            ColumnData::NullableUInt64(v) => {
                v.iter().map(|&e| {
                    match e {
                        Some(e) => e.to_string(),
                        None => "".to_string()
                    }
                }).collect()
            },
            ColumnData::NullableFloat32(v) => {
                v.iter().map(|&e| {
                    match e {
                        Some(e) => e.to_string(),
                        None => "".to_string()
                    }
                }).collect()
            },
            ColumnData::NullableFloat64(v) => {
                v.iter().map(|&e| {
                    match e {
                        Some(e) => e.to_string(),
                        None => "".to_string()
                    }
                }).collect()
            },
            ColumnData::NullableText(v) => {
                v.iter().map(|e| {
                    match e {
                        Some(e) => e.clone(),
                        None => "".to_string()
                    }
                }).collect()
            },
        }
    }

    pub fn remove(&mut self, index: usize) -> () {
        match self.column_data {
            ColumnData::Int8(ref mut v) => {
                v.remove(index);
            }
            ColumnData::Int16(ref mut v) => {
                v.remove(index);
            }
            ColumnData::Int32(ref mut v) => {
                v.remove(index);
            }
            ColumnData::Int64(ref mut v) => {
                v.remove(index);
            }
            ColumnData::UInt8(ref mut v) => {
                v.remove(index);
            }
            ColumnData::UInt16(ref mut v) => {
                v.remove(index);
            }
            ColumnData::UInt32(ref mut v) => {
                v.remove(index);
            }
            ColumnData::UInt64(ref mut v) => {
                v.remove(index);
            }
            ColumnData::Float32(ref mut v) => {
                v.remove(index);
            }
            ColumnData::Float64(ref mut v) => {
                v.remove(index);
            }
            ColumnData::Text(ref mut v) => {
                v.remove(index);
            }
            ColumnData::NullableInt8(ref mut v) => {
                v.remove(index);
            }
            ColumnData::NullableInt16(ref mut v) => {
                v.remove(index);
            }
            ColumnData::NullableInt32(ref mut v) => {
                v.remove(index);
            }
            ColumnData::NullableInt64(ref mut v) => {
                v.remove(index);
            }
            ColumnData::NullableUInt8(ref mut v) => {
                v.remove(index);
            }
            ColumnData::NullableUInt16(ref mut v) => {
                v.remove(index);
            }
            ColumnData::NullableUInt32(ref mut v) => {
                v.remove(index);
            }
            ColumnData::NullableUInt64(ref mut v) => {
                v.remove(index);
            }
            ColumnData::NullableFloat32(ref mut v) => {
                v.remove(index);
            }
            ColumnData::NullableFloat64(ref mut v) => {
                v.remove(index);
            }
            ColumnData::NullableText(ref mut v) => {
                v.remove(index);
            }
        };
    }
}

#[derive(Debug, PartialEq)]
pub enum ColumnData {
    Int8(Vec<i8>),
    Int16(Vec<i16>),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    UInt8(Vec<u8>),
    UInt16(Vec<u16>),
    UInt32(Vec<u32>),
    UInt64(Vec<u64>),
    Float32(Vec<f32>),
    Float64(Vec<f64>),
    Text(Vec<String>),
    NullableInt8(Vec<Option<i8>>),
    NullableInt16(Vec<Option<i16>>),
    NullableInt32(Vec<Option<i32>>),
    NullableInt64(Vec<Option<i64>>),
    NullableUInt8(Vec<Option<u8>>),
    NullableUInt16(Vec<Option<u16>>),
    NullableUInt32(Vec<Option<u32>>),
    NullableUInt64(Vec<Option<u64>>),
    NullableFloat32(Vec<Option<f32>>),
    NullableFloat64(Vec<Option<f64>>),
    NullableText(Vec<Option<String>>),
}


pub fn is_same_columndata_type(col_1: &ColumnData, col_2: &ColumnData) -> bool {
    match col_1 {
        ColumnData::Int8(_) => {
            match col_2 {
                ColumnData::Int8(_) => true,
                _ => false
            }
        },
        ColumnData::Int16(_) => {
            match col_2 {
                ColumnData::Int16(_) => true,
                _ => false
            }
        },
        ColumnData::Int32(_) => {
            match col_2 {
                ColumnData::Int32(_) => true,
                _ => false
            }
        },
        ColumnData::Int64(_) => {
            match col_2 {
                ColumnData::Int64(_) => true,
                _ => false
            }
        },
        ColumnData::UInt8(_) => {
            match col_2 {
                ColumnData::UInt8(_) => true,
                _ => false
            }
        },
        ColumnData::UInt16(_) => {
            match col_2 {
                ColumnData::UInt16(_) => true,
                _ => false
            }
        },
        ColumnData::UInt32(_) => {
            match col_2 {
                ColumnData::UInt32(_) => true,
                _ => false
            }
        },
        ColumnData::UInt64(_) => {
            match col_2 {
                ColumnData::UInt64(_) => true,
                _ => false
            }
        },
        ColumnData::Float32(_) => {
            match col_2 {
                ColumnData::Float32(_) => true,
                _ => false
            }
        },
        ColumnData::Float64(_) => {
            match col_2 {
                ColumnData::Float64(_) => true,
                _ => false
            }
        },
        ColumnData::Text(_) => {
            match col_2 {
                ColumnData::Text(_) => true,
                _ => false
            }
        },
        ColumnData::NullableInt8(_) => {
            match col_2 {
                ColumnData::NullableInt8(_) => true,
                _ => false
            }
        },
        ColumnData::NullableInt16(_) => {
            match col_2 {
                ColumnData::NullableInt16(_) => true,
                _ => false
            }
        },
        ColumnData::NullableInt32(_) => {
            match col_2 {
                ColumnData::NullableInt32(_) => true,
                _ => false
            }
        },
        ColumnData::NullableInt64(_) => {
            match col_2 {
                ColumnData::NullableInt64(_) => true,
                _ => false
            }
        },
        ColumnData::NullableUInt8(_) => {
            match col_2 {
                ColumnData::NullableUInt8(_) => true,
                _ => false
            }
        },
        ColumnData::NullableUInt16(_) => {
            match col_2 {
                ColumnData::NullableUInt16(_) => true,
                _ => false
            }
        },
        ColumnData::NullableUInt32(_) => {
            match col_2 {
                ColumnData::NullableUInt32(_) => true,
                _ => false
            }
        },
        ColumnData::NullableUInt64(_) => {
            match col_2 {
                ColumnData::NullableUInt64(_) => true,
                _ => false
            }
        },
        ColumnData::NullableFloat32(_) => {
            match col_2 {
                ColumnData::NullableFloat32(_) => true,
                _ => false
            }
        },
        ColumnData::NullableFloat64(_) => {
            match col_2 {
                ColumnData::NullableFloat64(_) => true,
                _ => false
            }
        },
        ColumnData::NullableText(_) => {
            match col_2 {
                ColumnData::NullableText(_) => true,
                _ => false
            }
        },
    }
}

pub enum Datum {
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Text(String),
    NullableInt8(Option<i8>),
    NullableInt16(Option<i16>),
    NullableInt32(Option<i32>),
    NullableInt64(Option<i64>),
    NullableUInt8(Option<u8>),
    NullableUInt16(Option<u16>),
    NullableUInt32(Option<u32>),
    NullableUInt64(Option<u64>),
    NullableFloat32(Option<f32>),
    NullableFloat64(Option<f64>),
    NullableText(Option<String>),
}

#[cfg(test)]
mod tests {
    use super::{Column, ColumnData, DataFrame, Datum};

    #[test]
    fn test_column_remove() {
        let mut col = Column::new("test".to_string(), ColumnData::Int8(vec![2, 3, 4, 5]));
        col.remove(1);

        assert_eq!(3, col.len());

        match col.column_data {
            ColumnData::Int8(v) => {
                assert_eq!(2, v[0]);
                assert_ne!(3, v[1]);
                assert_eq!(4, v[1]);
                assert_ne!(4, v[2]);
                assert_eq!(5, v[2]);
            },
            _ => {
                panic!("Unexpected ColumnData type");
            },
        };
    }

    #[test]
    fn test_dataframe_filter() {
        let df = DataFrame::from_vec(vec![
            Column::new("id".to_string(), ColumnData::Int8(vec![1, 2, 3, 4, 5])),
            Column::new(
                "label".to_string(),
                ColumnData::Text(vec![
                    "one".to_string(),
                    "two".to_string(),
                    "three".to_string(),
                    "four".to_string(),
                    "five".to_string(),
                ]),
            ),
            Column::new(
                "value".to_string(),
                ColumnData::NullableInt8(vec![
                    Some(64),
                    Some(49),
                    Some(36),
                    None,
                    Some(16),
                ])
            ),
        ]);

        assert_eq!(5, df.len());

        let df = df.drain_filter(|row, _| match row.get("id") {
            Some(value) => match value {
                Datum::Int8(id) => id % 2 != 0,
                _ => false,
            },
            None => false,
        });

        assert_eq!(3, df.len());
        
        match &df.columns[0].column_data {
            ColumnData::Int8(v) => {
                assert_eq!(1, v[0]);
                assert_eq!(3, v[1]);
                assert_eq!(5, v[2]);
            },
            _ => {
                panic!("Unexpected ColumnData type");
            },
        }
        match &df.columns[1].column_data {
            ColumnData::Text(v) => {
                assert_eq!("one".to_string(), v[0]);
                assert_eq!("three".to_string(), v[1]);
                assert_eq!("five".to_string(), v[2]);
            },
            _ => {
                panic!("Unexpected ColumnData type");
            },
        }
        match &df.columns[2].column_data {
            ColumnData::NullableInt8(v) => {
                assert_eq!(64, v[0].unwrap());
                assert_eq!(36, v[1].unwrap());
                assert_eq!(16, v[2].unwrap());
            },
            _ => {
                panic!("Unexpected ColumnData type");
            },
        }
    }
}
