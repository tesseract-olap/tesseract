use std::collections::HashMap;
use actix::SystemRunner;
use failure::{Error, format_err};
use log::info;

use serde_derive::Deserialize;

use tesseract_core::{Schema, Backend, ColumnData, Column};
use tesseract_core::names::{LevelName, Property};
use tesseract_core::schema::{Level, Cube, InlineTable};

use super::super::handlers::logic_layer::shared::{Time, TimePrecision, TimeValue};
use crate::logic_layer::LogicLayerConfig;


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
#[derive(Debug, Clone, Deserialize)]
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

    pub level_map: HashMap<String, LevelName>,
    pub property_map: HashMap<String, Property>,

    // Maps a level name to a `LevelCache` object
    pub level_caches: HashMap<String, LevelCache>,

    // Maps a dimension name to a `DimensionCache` object
    pub dimension_caches: HashMap<String, DimensionCache>,
}


impl CubeCache {
    pub fn get_time_cut(&self, time: Time) -> Result<(String, String), Error> {
        let (val_res, ln_res) = match time.precision {
            TimePrecision::Year => {
                let v = self.get_value(&time, self.year_values.clone());
                let l = self.get_level_name(self.year_level.clone());
                (v, l)
            },
            TimePrecision::Quarter => {
                let v = self.get_value(&time, self.quarter_values.clone());
                let l = self.get_level_name(self.quarter_level.clone());
                (v, l)
            },
            TimePrecision::Month => {
                let v = self.get_value(&time, self.month_values.clone());
                let l = self.get_level_name(self.month_level.clone());
                (v, l)
            },
            TimePrecision::Week => {
                let v = self.get_value(&time, self.week_values.clone());
                let l = self.get_level_name(self.week_level.clone());
                (v, l)
            },
            TimePrecision::Day => {
                let v = self.get_value(&time, self.day_values.clone());
                let l = self.get_level_name(self.day_level.clone());
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

    pub fn get_level_name(&self, level: Option<Level>) -> Option<String> {
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


#[derive(Debug, Clone, Deserialize)]
pub struct LevelCache {
    pub parent_map: Option<HashMap<String, String>>,
    pub children_map: Option<HashMap<String, Vec<String>>>,
}


#[derive(Debug, Clone, Deserialize)]
pub struct DimensionCache {
    pub id_map: HashMap<String, Vec<LevelName>>,
}


/// Populates a `Cache` object that will be shared through `AppState`.
pub fn populate_cache(
        schema: Schema,
        ll_config: &Option<LogicLayerConfig>,
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

        let mut level_caches: HashMap<String, LevelCache> = HashMap::new();
        let mut dimension_caches: HashMap<String, DimensionCache> = HashMap::new();

        for dimension in &cube.dimensions {
            let mut id_map: HashMap<String, Vec<LevelName>> = HashMap::new();

            for hierarchy in &dimension.hierarchies {
                let table = match &hierarchy.table {
                    Some(t) => &t.name,
                    None => &cube.table.name
                };

                for level in &hierarchy.levels {
                    if time_column_names.contains(&level.name) {
                        let val = get_distinct_values(
                            &level.key_column, &table, backend.clone(), sys
                        )?;

                        if level.name == "Year" {
                            year_level = Some(level.clone());
                            year_values = Some(val);
                        } else if level.name == "Quarter" {
                            quarter_level = Some(level.clone());
                            quarter_values = Some(val);
                        } else if level.name == "Month" {
                            month_level = Some(level.clone());
                            month_values = Some(val);
                        } else if level.name == "Week" {
                            week_level = Some(level.clone());
                            week_values = Some(val);
                        } else if level.name == "Day" {
                            day_level = Some(level.clone());
                            day_values = Some(val);
                        }
                    }

                    // Get unique name for this level
                    let unique_name = match get_unique_level_name(&cube, ll_config, &level)? {
                        Some(name) => name,
                        None => return Err(format_err!("Couldn't find unique name for {}", level.name.clone()))
                    };

                    let level_name = LevelName::new(
                        dimension.name.clone(),
                        hierarchy.name.clone(),
                        level.name.clone()
                    );

                    let mut parent_map: Option<HashMap<String, String>> = None;
                    let mut children_map: Option<HashMap<String, Vec<String>>> = None;

                    let parent_levels = cube.get_level_parents(&level_name)?;
                    let child_level = cube.get_child_level(&level_name)?;

                    let mut distinct_ids: Vec<String> = vec![];

                    if hierarchy.inline_table.is_some() {
                        // Inline table

                        let inline_table = match &hierarchy.inline_table {
                            Some(t) => t,
                            None => return Err(format_err!("Could not get inline table for {}", level.name.clone()))
                        };

                        if parent_levels.len() == 0 {
                            // No parents
                        } else {
                            parent_map = Some(get_inline_parent_data(
                                &parent_levels[parent_levels.len() - 1], &level,
                                &inline_table
                            ));
                        }

                        match child_level {
                            Some(child_level) => {
                                children_map = Some(get_inline_children_data(
                                    &level, &child_level, &inline_table
                                ));
                            },
                            None => ()
                        }

                        // Get all IDs for this level
                        for row in &inline_table.rows {
                            for row_value in &row.row_values {
                                if row_value.column == level.key_column {
                                    distinct_ids.push(row_value.value.clone());
                                }
                            }
                        }
                    } else {
                        // Database table

                        if parent_levels.len() == 0 {
                            // No parents
                        } else {
                            parent_map = Some(get_parent_data(
                                &parent_levels[parent_levels.len() - 1], &level,
                                table, backend.clone(), sys
                            )?);
                        }

                        match child_level {
                            Some(child_level) => {
                                children_map = Some(get_children_data(
                                    &level, &child_level,
                                    table, backend.clone(), sys
                                )?);
                            },
                            None => ()
                        }

                        // Get all IDs for this level
                        distinct_ids = get_distinct_values(
                            &level.key_column, &table, backend.clone(), sys
                        )?;
                    }

                    // Add each distinct ID to the id_map HashMap
                    for distinct_id in distinct_ids {
                        id_map.entry(distinct_id.clone()).or_insert(vec![]);
                        let map_entry = id_map.get_mut(&distinct_id).unwrap();
                        map_entry.push(level_name.clone());
                    }

                    level_caches.insert(unique_name.clone(), LevelCache { parent_map, children_map });
                }
            }

            dimension_caches.insert(dimension.name.clone(), DimensionCache { id_map });
        }

        let level_map = get_level_map(&cube, ll_config)?;
        let property_map = get_property_map(&cube, ll_config)?;

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
            level_map,
            property_map,
            level_caches,
            dimension_caches,
        })
    }

    info!("Cache ready!");

    Ok(Cache { cubes })
}

pub fn get_unique_level_name(cube: &Cube, ll_config: &Option<LogicLayerConfig>, level: &Level) -> Result<Option<String>, Error> {
    for dimension in &cube.dimensions {
        for hierarchy in &dimension.hierarchies {
            for curr_level in &hierarchy.levels {
                if curr_level == level {
                    let level_name = LevelName::new(
                        dimension.name.clone(),
                        hierarchy.name.clone(),
                        curr_level.name.clone()
                    );

                    let unique_level_name = match ll_config {
                        Some(ll_config) => {
                            let unique_level_name_opt = if dimension.is_shared {
                                ll_config.find_unique_shared_dimension_level_name(
                                    &dimension.name, &cube.name, &level_name
                                )?
                            } else {
                                ll_config.find_unique_cube_level_name(
                                    &cube.name, &level_name
                                )?
                            };

                            match unique_level_name_opt {
                                Some(unique_level_name) => unique_level_name,
                                None => curr_level.name.clone()
                            }
                        },
                        None => curr_level.name.clone()
                    };

                    return Ok(Some(unique_level_name))
                }
            }
        }
    }

    Ok(None)
}

pub fn get_level_map(cube: &Cube, ll_config: &Option<LogicLayerConfig>) -> Result<HashMap<String, LevelName>, Error> {
    let mut level_name_map = HashMap::new();

    for dimension in &cube.dimensions {
        for hierarchy in &dimension.hierarchies {
            for level in &hierarchy.levels {
                let level_name = LevelName::new(
                    dimension.name.clone(),
                    hierarchy.name.clone(),
                    level.name.clone()
                );

                let unique_level_name = match ll_config {
                    Some(ll_config) => {
                        let unique_level_name_opt = if dimension.is_shared {
                            ll_config.find_unique_shared_dimension_level_name(
                                &dimension.name, &cube.name, &level_name
                            )?
                        } else {
                            ll_config.find_unique_cube_level_name(
                                &cube.name, &level_name
                            )?
                        };

                        match unique_level_name_opt {
                            Some(unique_level_name) => unique_level_name,
                            None => level.name.clone()
                        }
                    },
                    None => level.name.clone()
                };

                level_name_map.insert(
                    unique_level_name.to_string(),
                    level_name
                );
            }
        }
    }

    Ok(level_name_map)
}

pub fn get_property_map(cube: &Cube, ll_config: &Option<LogicLayerConfig>) -> Result<HashMap<String, Property>, Error> {
    let mut property_map = HashMap::new();

    for dimension in &cube.dimensions {
        for hierarchy in &dimension.hierarchies {
            for level in &hierarchy.levels {
                if let Some(ref props) = level.properties {
                    for prop in props {
                        let property = Property::new(
                            dimension.name.clone(),
                            hierarchy.name.clone(),
                            level.name.clone(),
                            prop.name.clone()
                        );

                        let unique_property_name = match ll_config {
                            Some(ll_config) => {
                                let unique_property_name_opt = if dimension.is_shared {
                                    ll_config.find_unique_shared_dimension_property_name(
                                        &dimension.name, &cube.name, &property
                                    )?
                                } else {
                                    ll_config.find_unique_cube_property_name(
                                        &cube.name, &property
                                    )?
                                };

                                match unique_property_name_opt {
                                    Some(unique_property_name) => unique_property_name,
                                    None => prop.name.clone()
                                }
                            },
                            None => prop.name.clone()
                        };

                        property_map.insert(
                            unique_property_name.to_string(),
                            property
                        );
                    }
                }
            }
        }
    }

    Ok(property_map)
}

pub fn get_inline_parent_data(
        parent_level: &Level,
        current_level: &Level,
        inline_table: &InlineTable,
) -> HashMap<String, String> {
    let mut parent_data: HashMap<String, String> = HashMap::new();

    let mut parent_column: Vec<String> = vec![];
    let mut current_column: Vec<String> = vec![];

    for row in &inline_table.rows {
        for row_value in &row.row_values {
            if row_value.column == parent_level.key_column {
                parent_column.push(row_value.value.clone());
            } else if row_value.column == current_level.key_column {
                current_column.push(row_value.value.clone());
            }
        }
    }

    for i in 0..current_column.len() {
        parent_data.insert(current_column[i].clone(), parent_column[i].clone());
    }

    parent_data
}

pub fn get_inline_children_data(
        current_level: &Level,
        child_level: &Level,
        inline_table: &InlineTable,
) -> HashMap<String, Vec<String>> {
    let mut children_data: HashMap<String, Vec<String>> = HashMap::new();

    let mut current_column: Vec<String> = vec![];
    let mut children_column: Vec<String> = vec![];

    for row in &inline_table.rows {
        for row_value in &row.row_values {
            if row_value.column == current_level.key_column {
                current_column.push(row_value.value.clone());
            } else if row_value.column == child_level.key_column {
                children_column.push(row_value.value.clone());
            }
        }
    }

    let mut current_value: String = "".to_string();
    let mut current_children: Vec<String> = vec![];

    for i in 0..current_column.len() {
        if current_value == "".to_string() {
            current_value = current_column[i].clone();
            current_children.push(children_column[i].clone())
        } else {
            if current_column[i].clone() != current_value {
                children_data.insert(current_value.clone(), current_children.clone());
                current_children = vec![];
            }
            current_value = current_column[i].clone();
            current_children.push(children_column[i].clone())
        }
    }

    // Add last set of IDs
    children_data.insert(current_value, current_children);

    children_data
}

pub fn get_parent_data(
        parent_level: &Level,
        current_level: &Level,
        table: &str,
        backend: Box<dyn Backend + Sync + Send>,
        sys: &mut SystemRunner
) -> Result<HashMap<String, String>, Error> {
    let mut parent_data: HashMap<String, String> = HashMap::new();

    let future = backend
        .exec_sql(
            format!(
                "select distinct {}, {} from {} group by {}, {} order by {}, {}",
                parent_level.key_column, current_level.key_column, table,
                parent_level.key_column, current_level.key_column,
                parent_level.key_column, current_level.key_column,
            ).to_string()
        );

    let df = match sys.block_on(future) {
        Ok(df) => df,
        Err(err) => {
            return Err(format_err!("Error populating cache with backend data: {}", err));
        }
    };

    let parent_column = format_column_data(&df.columns[0]);
    let current_column = format_column_data(&df.columns[1]);

    for i in 0..current_column.len() {
        parent_data.insert(current_column[i].clone(), parent_column[i].clone());
    }

    Ok(parent_data)
}

pub fn get_children_data(
        current_level: &Level,
        child_level: &Level,
        table: &str,
        backend: Box<dyn Backend + Sync + Send>,
        sys: &mut SystemRunner
) -> Result<HashMap<String, Vec<String>>, Error> {
    let mut children_data: HashMap<String, Vec<String>> = HashMap::new();

    let future = backend
        .exec_sql(
            format!(
                "select distinct {}, {} from {} group by {}, {} order by {}, {}",
                current_level.key_column, child_level.key_column, table,
                current_level.key_column, child_level.key_column,
                current_level.key_column, child_level.key_column,
            ).to_string()
        );

    let df = match sys.block_on(future) {
        Ok(df) => df,
        Err(err) => {
            return Err(format_err!("Error populating cache with backend data: {}", err));
        }
    };

    let current_column = format_column_data(&df.columns[0]);
    let children_column = format_column_data(&df.columns[1]);

    let mut current_value: String = "".to_string();
    let mut current_children: Vec<String> = vec![];

    for i in 0..current_column.len() {
        if current_value == "".to_string() {
            current_value = current_column[i].clone();
            current_children.push(children_column[i].clone())
        } else {
            if current_column[i].clone() != current_value {
                children_data.insert(current_value.clone(), current_children.clone());
                current_children = vec![];
            }
            current_value = current_column[i].clone();
            current_children.push(children_column[i].clone())
        }
    }

    // Add last set of IDs
    children_data.insert(current_value, current_children);

    Ok(children_data)
}


/// Queries the database to get all the distinct values for a given level.
pub fn get_distinct_values(
        column: &str,
        table: &str,
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
        let values: Vec<String> = format_column_data(&df.columns[0]);
        return Ok(values);
    }

    return Ok(vec![]);
}


/// DataFrame columns can come in many different types. This function converts
/// all data to a common type (String).
pub fn format_column_data(col: &Column) -> Vec<String> {
    return match &col.column_data {
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
    }
}
