use failure::{Error, format_err};
use log::*;

use tesseract_core::{Schema, Cube, Dimension, Backend, ColumnData};
use tesseract_core::names::LevelName;

use super::super::handlers::logic_layer::shared::{Time, TimeValue};


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
    pub time_dim: Dimension,
    pub years: Vec<u32>,
}

impl CubeCache {
    /// Returns dimension name in the format: `Dimension.Hierarchy.Level`.
    pub fn get_time_level_name(&self) -> LevelName {
        LevelName {
            dimension: self.time_dim.name.clone(),
            hierarchy: self.time_dim.hierarchies[0].name.clone(),
            level: self.time_dim.hierarchies[0].levels[0].name.clone(),
        }
    }

    pub fn get_time_cut(&self, t: Time) -> Result<(String, String), Error> {
        let year_opt;

        // TODO: Add check for precision type
        match t.value {
            TimeValue::Last => year_opt = self.max_year(),
            TimeValue::First => year_opt = self.min_year(),
            TimeValue::Value(time) => year_opt = Some(time),
        }

        let year = match year_opt {
            None => { return Err(format_err!("Unable to get requested year.")); }
            Some(year) => year
        };

        let ln = self.get_time_level_name();

        Ok((ln.level().to_string(), year.to_string()))
    }

    pub fn min_year(&self) -> Option<u32> {
        if self.years.len() >= 1 {
            return Some(self.years[0]);
        }
        None
    }

    pub fn max_year(&self) -> Option<u32> {
        if self.years.len() >= 1 {
            return Some(*self.years.last().unwrap());
        }
        None
    }
}


/// Populates a `Cache` object that will be shared through `AppState`.
pub fn populate_cache(schema: Schema, backend: Box<dyn Backend + Sync + Send>) -> Result<Cache, Error> {
    info!("Populating cache...");

    let mut sys = actix::System::new("cache");
    let mut cubes: Vec<CubeCache> = vec![];

    for cube in schema.cubes {
        let preferred_time_dim = match find_years(cube.clone()) {
            Ok(r) => match r {
                Some(dim) => dim,
                None => continue
            },
            Err(_) => continue
        };
        let year_column = get_year_column(&preferred_time_dim);

        let future = backend
            .exec_sql(
                format!("select distinct {} from {}", year_column, cube.table.name)
                        .to_string()
            );
        let df = match sys.block_on(future) {
            Ok(df) => df,
            Err(err) => {
                return Err(format_err!("Error populating cache with backend data: {}", err));
            }
        };

        // TODO: Do we want to return an error if no columns are returned?
        if df.columns.len() >= 1 {
            let mut original_years = match &df.columns[0].column_data {
                ColumnData::Int8(v) => { let s: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect(); s },
                ColumnData::Int16(v) => { let s: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect(); s },
                ColumnData::Int32(v) => { let s: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect(); s },
                ColumnData::Int64(v) => { let s: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect(); s },
                ColumnData::UInt8(v) => { let s: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect(); s },
                ColumnData::UInt16(v) => { let s: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect(); s },
                ColumnData::UInt32(v) => { let s: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect(); s },
                ColumnData::UInt64(v) => { let s: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect(); s },
                ColumnData::Float32(v) => { let s: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect(); s },
                ColumnData::Float64(v) => { let s: Vec<u32> = v.iter().map(|&e| e.clone() as u32).collect(); s },
                ColumnData::Text(v) => { let s: Vec<u32> = v.iter().map(|e| e.parse::<u32>().unwrap().clone()).collect(); s },
            };

            original_years.sort();

            cubes.push(
                CubeCache {
                    name: cube.name.clone(),
                    time_dim: preferred_time_dim,
                    years: original_years
                }
            )
        }
    }

    info!("Cache ready!");

    Ok(Cache { cubes })
}

/// Helper to get the name of the year column in a given cube.
/// Right now it assumes that the first level provided containing the word
/// `year` in its `key_column` value is the one we're looking for.
/// If such level is not found, the method returns a default value of `year`.
pub fn get_year_column(dim: &Dimension) -> String {
    for hierarchy in &dim.hierarchies {
        if hierarchy.name.contains("Year") {
            for level in &hierarchy.levels {
                if level.key_column.contains("year") {
                    return level.key_column.clone();
                }
            }
        }
    }

    String::from("year")
}

/// Finds cubes and dimensions with year/time information.
/// The current logic is similar to the one for the existing Mondrian logic
/// layer, but we may want to change this in the future.
fn find_years(cube: Cube) -> Result<Option<Dimension>, Error> {
    let mut time_dimensions: Vec<Dimension> = vec![];

    for dimension in cube.dimensions {
        // TODO: implement and check for d.type and d.annotations
        if dimension.name.contains("Year") {
            time_dimensions.push(dimension);
        }
    }

    if time_dimensions.len() == 0 {
        return Ok(None);
    } else if time_dimensions.len() == 1 {
        return Ok(Some(time_dimensions[0].clone()));
    } else {
        for dim in &time_dimensions {
            if dim.name == "Year" {
                return Ok(Some(dim.clone()));
            }
        }

        for dim in &time_dimensions {
            if dim.name.contains("Year") {
                return Ok(Some(dim.clone()));
            }
        }
    }

    return Ok(Some(time_dimensions[0].clone()));
}
