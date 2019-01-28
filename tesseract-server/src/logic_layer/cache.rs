use log::*;
use std::collections::HashMap;

use tesseract_core::{Schema, Cube, Dimension};


/// Holds cache information.
#[derive(Debug, Clone)]
pub struct Cache {
    pub cube_info: Vec<CubeInfo>,
}

/// Holds cache information for a given cube.
#[derive(Debug, Clone)]
pub struct CubeInfo {
    pub name: String,
    pub years: HashMap<String, u32>,
}


/// Populates a cache with that will be shared in `AppState`
pub fn populate_cache(schema: Schema) -> Cache {
    info!("Populating cache...");

    let mut cube_info: Vec<CubeInfo> = vec![];

    for cube in schema.cubes {
        let preferred_time_dim = match find_years(cube.clone()) {
            Some(dim) => dim,
            None => { continue; }
        };

        // TODO: Use this dimension to get the most recent and latest year
        // println!("{:?}", preferred_time_dim);

        let mut years: HashMap<String, u32> = HashMap::new();

        years.insert("latest".to_string(), 2018);
        years.insert("oldest".to_string(), 2016);

        cube_info.push(
            CubeInfo {
                name: cube.name.clone(),
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
