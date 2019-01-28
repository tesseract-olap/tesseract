use log::*;

use tesseract_core::{Schema, Cube, Dimension};


/// Populates a cache with that will be shared in `AppState`
pub fn populate_cache(schema: Schema) {
    info!("Populating cache...");

    // TODO: Get a list of all the cubes
    for cube in schema.cubes {
        let preferred_time_dim = find_years(cube.clone());

        println!("{}", cube.name);
        println!("{:?}", preferred_time_dim);
    }

    // TODO: Create HashMap with measures and metadata

    // TODO: Find cubes with a year/time dimension and store relevant information

    // TODO: Create shared state object and add it to `AppState`

    info!("Cache ready!");
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
