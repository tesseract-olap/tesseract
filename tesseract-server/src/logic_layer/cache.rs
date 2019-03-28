use actix::SystemRunner;
use failure::{Error, format_err};
use log::info;

use tesseract_core::{Schema, Backend, ColumnData};
use tesseract_core::schema::Level;

use super::super::handlers::logic_layer::shared::{Time, TimePrecision, TimeValue};


/// Holds cache information.
#[derive(Debug, Clone)]
pub struct Cache {
    pub cubes: Vec<CubeCache>,
}

impl Cache {
    /// Finds the `CubeCache` object for a cube with a given name.
    pub fn find_cube_info(&self, cube: &String) -> Option<CubeCache> {
        for cube_cache in &self.cubes {
            if cube_cache.name == *cube {
                return Some(cube_cache.clone());
            }
        }
        None
    }
}

/// Holds cache information for a given cube.
#[derive(Debug, Clone)]
pub struct CubeCache {
    pub name: String,

    pub year_level: Option<Level>,
    pub year_values: Option<Vec<String>>,

    pub quarter_level: Option<Level>,
    pub quarter_values: Option<Vec<String>>,

    pub month_level: Option<Level>,
    pub month_values: Option<Vec<String>>,

    pub week_level: Option<Level>,
    pub week_values: Option<Vec<String>>,

    pub day_level: Option<Level>,
    pub day_values: Option<Vec<String>>,
}

impl CubeCache {
    pub fn get_time_cut(&self, time: Time) -> Result<(String, String), Error> {
        let (val_res, ln_res) = match time.precision {
            TimePrecision::Year => {
                let v = self.get_value(&time, self.year_values.clone());
                let l = self.get_level_name(&time, self.year_level.clone());
                (v, l)
            },
            TimePrecision::Quarter => {
                let v = self.get_value(&time, self.quarter_values.clone());
                let l = self.get_level_name(&time, self.quarter_level.clone());
                (v, l)
            },
            TimePrecision::Month => {
                let v = self.get_value(&time, self.month_values.clone());
                let l = self.get_level_name(&time, self.month_level.clone());
                (v, l)
            },
            TimePrecision::Week => {
                let v = self.get_value(&time, self.week_values.clone());
                let l = self.get_level_name(&time, self.week_level.clone());
                (v, l)
            },
            TimePrecision::Day => {
                let v = self.get_value(&time, self.day_values.clone());
                let l = self.get_level_name(&time, self.day_level.clone());
                (v, l)
            }
        };

        let val = match val_res {
            Some(o) => o,
            None => return Err(format_err!("Unable to get requested time precision data."))
        };

        let ln = match ln_res {
            Some(o) => o,
            None => return Err(format_err!("Unable to get requested time precision level name."))
        };

        Ok((ln, val))
    }

    pub fn get_level_name(&self, time: &Time, level: Option<Level>) -> Option<String> {
        match level {
            Some(l) => Some(l.name),
            None => None
        }
    }

    pub fn get_value(&self, time: &Time, opt: Option<Vec<String>>) -> Option<String> {
        match opt {
            Some(v) => {
                match time.value {
                    TimeValue::First => {
                        if v.len() >= 1 {
                            return Some(v[0].clone());
                        }
                        None
                    },
                    TimeValue::Last => {
                        if v.len() >= 1 {
                            return Some(v.last().unwrap().clone())
                        }
                        None
                    },
                    TimeValue::Value(t) => return Some(t.to_string())
                }
            },
            None => None
        }
    }
}


/// Populates a `Cache` object that will be shared through `AppState`.
pub fn populate_cache(
        schema: Schema,
        backend: Box<dyn Backend + Sync + Send>,
        sys: &mut SystemRunner
) -> Result<Cache, Error> {
    info!("Populating cache...");

    let time_column_names = vec![
        "Year".to_string(),
        "Quarter".to_string(),
        "Month".to_string(),
        "Week".to_string(),
        "Day".to_string()
    ];

    let mut cubes: Vec<CubeCache> = vec![];

    for cube in schema.cubes {
        let mut year_level: Option<Level> = None;
        let mut year_values: Option<Vec<String>> = None;
        let mut quarter_level: Option<Level> = None;
        let mut quarter_values: Option<Vec<String>> = None;
        let mut month_level: Option<Level> = None;
        let mut month_values: Option<Vec<String>> = None;
        let mut week_level: Option<Level> = None;
        let mut week_values: Option<Vec<String>> = None;
        let mut day_level: Option<Level> = None;
        let mut day_values: Option<Vec<String>> = None;

        for dimension in cube.dimensions.clone() {
            for hierarchy in dimension.hierarchies.clone() {
                let table = match hierarchy.table {
                    Some(t) => t.name,
                    None => cube.table.name.clone()
                };

                for level in hierarchy.levels.clone() {
                    if time_column_names.contains(&level.name) {
                        let values_res = get_time_values(
                            level.key_column.clone(),
                            table.clone(),
                            backend.clone(),
                            sys
                        );

                        match values_res {
                            Ok(val) => {
                                if level.name == "Year" {
                                    year_level = Some(level);
                                    year_values = Some(val);
                                } else if level.name == "Quarter" {
                                    quarter_level = Some(level);
                                    quarter_values = Some(val);
                                } else if level.name == "Month" {
                                    month_level = Some(level);
                                    month_values = Some(val);
                                } else if level.name == "Week" {
                                    week_level = Some(level);
                                    week_values = Some(val);
                                } else if level.name == "Day" {
                                    day_level = Some(level);
                                    day_values = Some(val);
                                }
                            },
                            Err(err) => return Err(err)
                        };
                    }
                }
            }
        }

        cubes.push(CubeCache {
            name: cube.name,
            year_level,
            year_values,
            quarter_level,
            quarter_values,
            month_level,
            month_values,
            week_level,
            week_values,
            day_level,
            day_values,
        })
    }

    info!("Cache ready!");

    Ok(Cache { cubes })
}


/// Queries the database to get all the distinct values for a given time level.
pub fn get_time_values(
        column: String,
        table: String,
        backend: Box<dyn Backend + Sync + Send>,
        sys: &mut SystemRunner
) -> Result<Vec<String>, Error> {
    let future = backend
        .exec_sql(
            format!("select distinct {} from {}", column, table).to_string()
        );
    let df = match sys.block_on(future) {
        Ok(df) => df,
        Err(err) => {
            return Err(format_err!("Error populating cache with backend data: {}", err));
        }
    };

    if df.columns.len() >= 1 {
        let values: Vec<String> = match &df.columns[0].column_data {
            ColumnData::Int8(v) => {
                let mut t: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::Int16(v) => {
                let mut t: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::Int32(v) => {
                let mut t: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::Int64(v) => {
                let mut t: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::UInt8(v) => {
                let mut t: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::UInt16(v) => {
                let mut t: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::UInt32(v) => {
                let mut t: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::UInt64(v) => {
                let mut t: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::Float32(v) => {
                let mut t: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::Float64(v) => {
                let mut t: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::Text(v) => {
                let mut t = v.to_vec();
                t.sort();
                t
            },
            ColumnData::NullableInt8(v) => {
                let mut t: Vec<u32> = v.iter().filter_map(|&e| e.map(|e| e.clone() as u32)).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::NullableInt16(v) => {
                let mut t: Vec<u32> = v.iter().filter_map(|&e| e.map(|e| e.clone() as u32)).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::NullableInt32(v) => {
                let mut t: Vec<u32> = v.iter().filter_map(|&e| e.map(|e| e.clone() as u32)).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::NullableInt64(v) => {
                let mut t: Vec<u32> = v.iter().filter_map(|&e| e.map(|e| e.clone() as u32)).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::NullableUInt8(v) => {
                let mut t: Vec<u32> = v.iter().filter_map(|&e| e.map(|e| e.clone() as u32)).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::NullableUInt16(v) => {
                let mut t: Vec<u32> = v.iter().filter_map(|&e| e.map(|e| e.clone() as u32)).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::NullableUInt32(v) => {
                let mut t: Vec<u32> = v.iter().filter_map(|&e| e.map(|e| e.clone() as u32)).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::NullableUInt64(v) => {
                let mut t: Vec<u32> = v.iter().filter_map(|&e| e.map(|e| e.clone() as u32)).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::NullableFloat32(v) => {
                let mut t: Vec<u32> = v.iter().filter_map(|&e| e.map(|e| e.clone() as u32)).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::NullableFloat64(v) => {
                let mut t: Vec<u32> = v.iter().filter_map(|&e| e.map(|e| e.clone() as u32)).collect();
                t.sort();
                t.iter().map(|&e| e.to_string()).collect()
            },
            ColumnData::NullableText(v) => {
                let mut t: Vec<_> = v.into_iter().filter_map(|e| e.clone()).collect();
                t.sort();
                t
            },
        };

        return Ok(values);
    }

    return Ok(vec![]);
}
