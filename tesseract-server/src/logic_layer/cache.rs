use log::*;
use std::collections::HashMap;

use tesseract_core::{Schema, Cube, Dimension, Backend, DataFrame, ColumnData};
//use tesseract_core::Query as TsQuery;


/// Holds cache information.
#[derive(Debug, Clone)]
pub struct Cache {
    pub cube_info: Vec<CubeInfo>,
}

impl Cache {
    /// Finds the CubeInfo object for a cube with a given name.
    pub fn find_cube_info(&self, cube: &String) -> Option<CubeInfo> {
        for cube_info in &self.cube_info {
            if cube_info.name == *cube {
                return Some(cube_info.clone());
            }
        }
        return None;
    }
}

/// Holds cache information for a given cube.
#[derive(Debug, Clone)]
pub struct CubeInfo {
    pub name: String,
    pub time_dim: Dimension,
    pub years: HashMap<String, i16>,
}

impl CubeInfo {
    /// Returns dimension name in the format: `Dimension.Hierarchy.Level`.
    pub fn get_time_dim_name(&self) -> String {
        format!("{}.{}.{}",
            self.time_dim.name,
            self.time_dim.hierarchies[0].name,
            self.time_dim.hierarchies[0].levels[0].name,
        ).to_string()
    }

    pub fn get_year_cut(&self, s: String) -> String {
        format!("{}.{}", self.get_time_dim_name(), self.years[&s.clone()]).to_string()
    }
}


/// Populates a `Cache` with that will be shared through `AppState`.
pub fn populate_cache(schema: Schema, backend: Box<dyn Backend + Sync + Send>) -> Cache {
    info!("Populating cache...");

    let mut cube_info: Vec<CubeInfo> = vec![];

    for cube in schema.cubes {
        let preferred_time_dim = match find_years(cube.clone()) {
            Some(dim) => dim,
            None => { continue; }
        };

        let mut sys = actix::System::new("cache");

        let future = backend
            .exec_sql("select distinct year from example".to_string());

        let df = sys.block_on(future).unwrap();

        let mut original_years = match &df.columns[0].column_data {
            // TODO: Refactor
            ColumnData::Int8(v) => {
                let mut temp: Vec<i16> = vec![];
                for x in v {
                    temp.push(x.clone() as i16)
                }
                temp
            },
            ColumnData::Int16(v) => {
                let mut temp: Vec<i16> = vec![];
                for x in v {
                    temp.push(x.clone() as i16)
                }
                temp
            },
            ColumnData::Int32(v) => {
                let mut temp: Vec<i16> = vec![];
                for x in v {
                    temp.push(x.clone() as i16)
                }
                temp
            },
            _ => panic!("Something")
        };

        original_years.sort();

        let mut years: HashMap<String, i16> = HashMap::new();
        years.insert("latest".to_string(), original_years.last().unwrap().clone());
        years.insert("oldest".to_string(), original_years[0]);

        println!("{:?}", years);

        cube_info.push(
            CubeInfo {
                name: cube.name.clone(),
                time_dim: preferred_time_dim,
                years
            }
        )
    }

    info!("Cache ready!");

    Cache { cube_info }
}

/// Finds cubes and dimensions with year/time information
fn find_years(cube: Cube) -> Option<Dimension> {
    let mut time_dimensions: Vec<Dimension> = vec![];

    for dimension in cube.dimensions {
        // TODO: implement and check for d.type and d.annotations
        if dimension.name.contains("Year") {
            time_dimensions.push(dimension);
        }
    }

    if time_dimensions.len() == 0 {
        return None;
    } else if time_dimensions.len() == 1 {
        return Some(time_dimensions[0].clone());
    } else {
        // TODO: Refactor
        for dim in &time_dimensions {
            if dim.name == "Year" {
                return Some(dim.clone());
            }
        }

        for dim in &time_dimensions {
            if dim.name.contains("End") {
                return Some(dim.clone());
            }
        }

        for dim in &time_dimensions {
            if dim.name.contains("Year") {
                return Some(dim.clone());
            }
        }
    }

    return Some(time_dimensions[0].clone());
}
