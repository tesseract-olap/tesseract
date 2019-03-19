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
    pub year_values: Option<Vec<u32>>,
    pub quarter_level: Option<Level>,
    pub quarter_values: Option<Vec<u32>>,
    pub month_level: Option<Level>,
    pub month_values: Option<Vec<u32>>,
    pub week_level: Option<Level>,
    pub week_values: Option<Vec<u32>>,
    pub day_level: Option<Level>,
    pub day_values: Option<Vec<u32>>,
}

impl CubeCache {
    pub fn get_time_cut(&self, t: Time) -> Result<(String, String), Error> {
        let val = match self.get_value(t.clone()) {
            None => { return Err(format_err!("Unable to get requested time precision data.")); }
            Some(o) => o.to_string()
        };

        let ln = match self.get_level_name(t) {
            Some(o) => o,
            None => { return Err(format_err!("Unable to get requested time precision level name.")); }
        };

        Ok((ln, val))
    }

    pub fn get_level_name(&self, time: Time) -> Option<String> {
        match time.precision {
            TimePrecision::Year => {
                match self.year_level.clone() {
                    Some(ln) => Some(ln.name),
                    None => None
                }
            },
            TimePrecision::Quarter => {
                match self.quarter_level.clone() {
                    Some(ln) => Some(ln.name),
                    None => None
                }
            },
            TimePrecision::Month => {
                match self.month_level.clone() {
                    Some(ln) => Some(ln.name),
                    None => None
                }
            },
            TimePrecision::Week => {
                match self.week_level.clone() {
                    Some(ln) => Some(ln.name),
                    None => None
                }
            },
            TimePrecision::Day => {
                match self.day_level.clone() {
                    Some(ln) => Some(ln.name),
                    None => None
                }
            },
        }
    }

    pub fn get_value(&self, time: Time) -> Option<u32> {
        match time.precision {
            TimePrecision::Year => {
                match self.year_values.clone() {
                    Some(v) => {
                        match time.value {
                            TimeValue::First => return Some(v[0]),
                            TimeValue::Last => return Some(*v.last().unwrap()),
                            TimeValue::Value(t) => return Some(t)
                        }
                    },
                    None => None
                }
            },
            TimePrecision::Quarter => {
                match self.quarter_values.clone() {
                    Some(v) => {
                        match time.value {
                            TimeValue::First => return Some(v[0]),
                            TimeValue::Last => return Some(*v.last().unwrap()),
                            TimeValue::Value(t) => return Some(t)
                        }
                    },
                    None => None
                }
            },
            TimePrecision::Month => {
                match self.month_values.clone() {
                    Some(v) => {
                        match time.value {
                            TimeValue::First => return Some(v[0]),
                            TimeValue::Last => return Some(*v.last().unwrap()),
                            TimeValue::Value(t) => return Some(t)
                        }
                    },
                    None => None
                }
            },
            TimePrecision::Week => {
                match self.week_values.clone() {
                    Some(v) => {
                        match time.value {
                            TimeValue::First => return Some(v[0]),
                            TimeValue::Last => return Some(*v.last().unwrap()),
                            TimeValue::Value(t) => return Some(t)
                        }
                    },
                    None => None
                }
            },
            TimePrecision::Day => {
                match self.day_values.clone() {
                    Some(v) => {
                        match time.value {
                            TimeValue::First => return Some(v[0]),
                            TimeValue::Last => return Some(*v.last().unwrap()),
                            TimeValue::Value(t) => return Some(t)
                        }
                    },
                    None => None
                }
            },
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
        let mut year_values: Option<Vec<u32>> = None;
        let mut quarter_level: Option<Level> = None;
        let mut quarter_values: Option<Vec<u32>> = None;
        let mut month_level: Option<Level> = None;
        let mut month_values: Option<Vec<u32>> = None;
        let mut week_level: Option<Level> = None;
        let mut week_values: Option<Vec<u32>> = None;
        let mut day_level: Option<Level> = None;
        let mut day_values: Option<Vec<u32>> = None;

        for dimension in cube.dimensions.clone() {
            for hierarchy in dimension.hierarchies.clone() {
                for level in hierarchy.levels.clone() {
                    if time_column_names.contains(&level.name) {
                        let values_res = get_time_values(
                            level.key_column.clone(),
                            cube.table.name.clone(),
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

pub fn get_time_values(
        column: String,
        table: String,
        backend: Box<dyn Backend + Sync + Send>,
        sys: &mut SystemRunner
) -> Result<Vec<u32>, Error> {
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
        let mut values: Vec<u32> = match &df.columns[0].column_data {
            ColumnData::Int8(v) => {
                v.iter().map(|&e| e.clone() as u32).collect()
            },
            ColumnData::Int16(v) => {
                v.iter().map(|&e| e.clone() as u32).collect()
            },
            ColumnData::Int32(v) => {
                v.iter().map(|&e| e.clone() as u32).collect()
            },
            ColumnData::Int64(v) => {
                v.iter().map(|&e| e.clone() as u32).collect()
            },
            ColumnData::UInt8(v) => {
                v.iter().map(|&e| e.clone() as u32).collect()
            },
            ColumnData::UInt16(v) => {
                v.iter().map(|&e| e.clone() as u32).collect()
            },
            ColumnData::UInt32(v) => {
                v.iter().map(|&e| e.clone() as u32).collect()
            },
            ColumnData::UInt64(v) => {
                v.iter().map(|&e| e.clone() as u32).collect()
            },
            ColumnData::Float32(v) => {
                v.iter().map(|&e| e.clone() as u32).collect()
            },
            ColumnData::Float64(v) => {
                v.iter().map(|&e| e.clone() as u32).collect()
            },
            ColumnData::Text(v) => {
                // TODO: Add better support for text types
                v.iter().map(|e| e.parse::<u32>().unwrap().clone()).collect()
            },
        };

        values.sort();

        return Ok(values);
    }

    return Ok(vec![]);
}
