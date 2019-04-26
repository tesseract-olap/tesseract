use failure::{Error, format_err};
use std::collections::HashSet;

use serde_derive::Deserialize;
use serde_json;
use tesseract_core::Schema;
use tesseract_core::names::{LevelName, Property};
use futures::future::Err;


#[derive(Debug, Clone, Deserialize)]
pub struct LogicLayerConfig {
    pub aliases: Option<AliasConfig>,
    pub named_sets: Option<Vec<NamedSetsConfig>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AliasConfig {
    pub cubes: Option<Vec<CubeAliasConfig>>,
    pub shared_dimensions: Option<Vec<SharedDimensionAliasConfig>>
}

#[derive(Debug, Clone, Deserialize)]
pub struct CubeAliasConfig {
    pub name: String,
    pub alternatives: Vec<String>,
    pub levels: Option<Vec<LevelPropertyConfig>>,
    pub properties: Option<Vec<LevelPropertyConfig>>
}

#[derive(Debug, Clone, Deserialize)]
pub struct SharedDimensionAliasConfig {
    pub name: String,
    pub levels: Option<Vec<LevelPropertyConfig>>,
    pub properties: Option<Vec<LevelPropertyConfig>>
}

#[derive(Debug, Clone, Deserialize)]
pub struct NamedSetsConfig {
    pub level_name: String,
    pub sets: Vec<NamedSetConfig>
}

#[derive(Debug, Clone, Deserialize)]
pub struct NamedSetConfig {
    pub set_name: String,
    pub values: Vec<String>
}

// TODO: Might want to consider eventually sharing the same structure as the current CubeAliasConfig
#[derive(Debug, Clone, Deserialize)]
pub struct LevelPropertyConfig {
    pub current_name: String,
    pub unique_name: String
}


trait FindUnique {
    fn find_unique_level_name(&self, level_name: &LevelName) -> Result<Option<&str>, Error>;
    fn find_unique_property_name(&self, level_name: &Property) -> Result<Option<&str>, Error>;

//    fn find_unique_level_name(&self, level_name: &LevelName) -> Result<Option<&str>, Error> {
//        let levels_opt: Option<Vec<LevelPropertyConfig>> = &self.levels;
//        if let Some(levels) = levels_opt {
//            for level in levels {
//                let ll_level_name: LevelName = level.current_name.parse()?;
//
//                if &ll_level_name == level_name {
//                    return Ok(Some(&level.unique_name))
//                }
//            }
//        }
//        Ok(None)
//    }
//
//    fn find_unique_property_name(&self, property_name: &Property) -> Result<Option<&str>, Error> {
//        let properties_opt: Option<Vec<LevelPropertyConfig>> = &self.properties;
//        if let Some(properties) = properties_opt {
//            for property in properties {
//                let ll_property_name: Property = property.current_name.parse()?;
//
//                if &ll_property_name == property_name {
//                    return Ok(Some(&property.unique_name))
//                }
//            }
//        }
//        Ok(None)
//    }
}

impl FindUnique for CubeAliasConfig {
    fn find_unique_level_name(&self, level_name: &LevelName) -> Result<Option<&str>, Error> {
        if let Some(levels) = &self.levels {
            for level in levels {
                let ll_level_name: LevelName = level.current_name.parse()?;

                if &ll_level_name == level_name {
                    return Ok(Some(&level.unique_name))
                }
            }
        }
        Ok(None)
    }

    fn find_unique_property_name(&self, property_name: &Property) -> Result<Option<&str>, Error> {
        if let Some(properties) = &self.properties {
            for property in properties {
                let ll_property_name: Property = property.current_name.parse()?;

                if &ll_property_name == property_name {
                    return Ok(Some(&property.unique_name))
                }
            }
        }
        Ok(None)
    }
}

impl FindUnique for SharedDimensionAliasConfig {
    fn find_unique_level_name(&self, level_name: &LevelName) -> Result<Option<&str>, Error> {
        if let Some(levels) = &self.levels {
            for level in levels {
                let ll_level_name: LevelName = level.current_name.parse()?;

                if &ll_level_name == level_name {
                    return Ok(Some(&level.unique_name))
                }
            }
        }
        Ok(None)
    }

    fn find_unique_property_name(&self, property_name: &Property) -> Result<Option<&str>, Error> {
        if let Some(properties) = &self.properties {
            for property in properties {
                let ll_property_name: Property = property.current_name.parse()?;

                if &ll_property_name == property_name {
                    return Ok(Some(&property.unique_name))
                }
            }
        }
        Ok(None)
    }
}


trait DeconstructAlias {
    fn find_level_name(&self, level_name: &str) -> Option<&str>;
    fn find_property_name(&self, level_name: &str) -> Option<&str>;
}

impl DeconstructAlias for CubeAliasConfig {
    fn find_level_name(&self, level_name: &str) -> Option<&str> {
        if let Some(levels) = &self.levels {
            for level in levels {
                if &level.unique_name == level_name {
                    return Some(&level.current_name)
                }
            }
        }
        None
    }

    fn find_property_name(&self, property_name: &str) -> Option<&str> {
        if let Some(properties) = &self.properties {
            for property in properties {
                if &property.unique_name == property_name {
                    return Some(&property.current_name)
                }
            }
        }
        None
    }
}

impl DeconstructAlias for SharedDimensionAliasConfig {
    fn find_level_name(&self, level_name: &str) -> Option<&str> {
        if let Some(levels) = &self.levels {
            for level in levels {
                if &level.unique_name == level_name {
                    return Some(&level.current_name)
                }
            }
        }
        None
    }

    fn find_property_name(&self, property_name: &str) -> Option<&str> {
        if let Some(properties) = &self.properties {
            for property in properties {
                if &property.unique_name == property_name {
                    return Some(&property.current_name)
                }
            }
        }
        None
    }
}


/// Reads Logic Layer Config JSON file.
pub fn read_config(config_path: &String) -> Result<LogicLayerConfig, Error> {
    let config_str = std::fs::read_to_string(&config_path)
        .map_err(|_| format_err!("Logic layer config file not found at {}", config_path))?;

    let config = match serde_json::from_str::<LogicLayerConfig>(&config_str) {
        Ok(config) => config,
        Err(err) => {
            return Err(format_err!("Unable to read logic layer config at {}: {}", config_path, err))
        }
    };

    if let Some(named_sets) = &config.named_sets {
        let mut set_names = HashSet::new();

        for named_set in named_sets.iter() {
            for set in named_set.sets.iter() {
                if !set_names.insert(set.set_name.clone()) {
                    return Err(format_err!("Make sure the logic layer config has unique set names"))
                }
            }
        }
        return Ok(config)
    } else {
        return Ok(config)
    }
}

impl LogicLayerConfig {
    /// Given a cube name, loops over the LogicLayerConfig and returns the
    /// actual cube name if an alias was provided.
    pub fn sub_cube_name(self, name: String) -> Result<String, Error> {
        match self.aliases {
            Some(aliases) => {
                match aliases.cubes {
                    Some(cubes) => {
                        for cube in cubes {
                            for alt in cube.alternatives {
                                if alt == name {
                                    return Ok(cube.name);
                                }
                            }
                        }
                        return Ok(name)
                    },
                    None => return Ok(name)
                }
            },
            None => return Ok(name)
        };
    }

    /// Given a drilldown level name, try to match that to one of the config
    /// named set names. If there is a match, return the associated level name
    /// for that named set.
    pub fn substitute_drill_value(self, level_name: String) -> Option<String> {
        if let Some(named_sets) = &self.named_sets {
            for named_set in named_sets.iter() {
                for set in named_set.sets.iter() {
                    if set.set_name == level_name {
                        return Some(named_set.level_name.clone())
                    }
                }
            }
        }
        None
    }

    /// Given a cut string, find if that matches any of the substitutions
    /// defined in `named_sets`. If so, substitute the cut value.
    pub fn substitute_cut(self, level_name: String, cut: String) -> String {
        match self.named_sets {
            Some(named_sets) => {
                let cuts: Vec<String> = cut.split(",").map(|s| s.to_string()).collect();

                let mut final_cuts: Vec<String> = vec![];

                for c in cuts.clone() {
                    let mut found = false;

                    for named_set in named_sets.clone() {
                        if named_set.level_name == level_name {
                            for set in named_set.sets.clone() {
                                if c == set.set_name {
                                    final_cuts.extend(set.values);
                                    found = true;
                                    break;
                                }
                            }
                        }
                    }

                    // No substitutions found, so just add the raw cut
                    if found == false {
                        final_cuts.push(c);
                    }
                }

                final_cuts.join(",")
            },
            None => cut
        }
    }

    /// Returns a unique name definition for a given cube level if there is one.
    pub fn find_unique_cube_level_name(
        &self, cube_name: &String, level_name: &LevelName
    ) -> Result<Option<&str>, Error> {
        if let Some(aliases) = &self.aliases {
            if let Some(cubes) = &aliases.cubes {
                for cube in cubes {
                    if &cube.name == cube_name {
                        let res = cube.find_unique_level_name(&level_name)?;
                        if let Some(res) = res {
                            return Ok(Some(res))
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    /// Returns a unique name definition for a given shared dimension level if there is one.
    pub fn find_unique_shared_dimension_level_name(
        &self, shared_dimension_name: &String, level_name: &LevelName
    ) -> Result<Option<&str>, Error> {
        if let Some(aliases) = &self.aliases {
            if let Some(shared_dimensions) = &aliases.shared_dimensions {
                for shared_dimension in shared_dimensions {
                    if &shared_dimension.name == shared_dimension_name {
                        let res = shared_dimension.find_unique_level_name(&level_name)?;
                        if let Some(res) = res {
                            return Ok(Some(res))
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    /// Returns a unique name definition for a given cube property if there is one.
    pub fn find_unique_cube_property_name(
        &self, cube_name: &String, property_name: &Property
    ) -> Result<Option<&str>, Error> {
        if let Some(aliases) = &self.aliases {
            if let Some(cubes) = &aliases.cubes {
                for cube in cubes {
                    if &cube.name == cube_name {
                        let res = cube.find_unique_property_name(&property_name)?;
                        if let Some(res) = res {
                            return Ok(Some(res))
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    /// Returns a unique name definition for a given shared dimension property if there is one.
    pub fn find_unique_shared_dimension_property_name(
        &self, shared_dimension_name: &String, property_name: &Property
    ) -> Result<Option<&str>, Error> {
        if let Some(aliases) = &self.aliases {
            if let Some(shared_dimensions) = &aliases.shared_dimensions {
                for shared_dimension in shared_dimensions {
                    if &shared_dimension.name == shared_dimension_name {
                        let res = shared_dimension.find_unique_property_name(&property_name)?;
                        if let Some(res) = res {
                            return Ok(Some(res))
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    /// Ensures level and property names are unique inside each cube based on
    /// name substitutions from a logic layer configuration.
    pub fn has_unique_levels_properties(&self, schema: &Schema) -> Result<bool, Error> {
        for cube in &schema.cubes {
            let mut levels = HashSet::new();
            let mut properties = HashSet::new();

            for dimension in &cube.dimensions {
//                println!("{:?}", dimension.name);
//                println!("{:?}", dimension.is_shared);

                for hierarchy in &dimension.hierarchies {

                    // Check each cube for unique level and property names
                    for level in &hierarchy.levels {
                        let level_name = LevelName::new(
                            dimension.name.clone(),
                            hierarchy.name.clone(),
                            level.name.clone()
                        );

                        let unique_level_name_opt = if dimension.is_shared {
                            self.find_unique_shared_dimension_level_name(
                                &dimension.name, &level_name
                            )?
                        } else {
                            self.find_unique_cube_level_name(
                                &cube.name, &level_name
                            )?
                        };

                        let unique_level_name = match unique_level_name_opt {
                            Some(unique_level_name) => unique_level_name,
                            None => &level.name
                        };

                        if !levels.insert(unique_level_name) {
                            return Ok(false)
                        }

                        if let Some(ref props) = level.properties {
                            for property in props {
                                let property_name = Property::new(
                                    dimension.name.clone(),
                                    hierarchy.name.clone(),
                                    level.name.clone(),
                                    property.name.clone()
                                );

                                let unique_property_name_opt = if dimension.is_shared {
                                    self.find_unique_shared_dimension_property_name(
                                        &dimension.name, &property_name
                                    )?
                                } else {
                                    self.find_unique_cube_property_name(
                                        &cube.name, &property_name
                                    )?
                                };

                                let unique_property_name = match unique_property_name_opt {
                                    Some(unique_property_name) => unique_property_name,
                                    None => &property.name
                                };

                                if !properties.insert(unique_property_name) {
                                    return Ok(false)
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(true)
    }

    pub fn find_cube_property_name(&self, cube_name: &String, property_name: &str) -> Option<&str> {
        if let Some(aliases) = &self.aliases {
            if let Some(cubes) = &aliases.cubes {
                for cube in cubes {
                    if &cube.name == cube_name {
                        let res = cube.find_level_name(&property_name);
                        if let Some(res) = res {
                            return Some(res)
                        }
                    }
                }
            }
        }
        None
    }

    pub fn find_shared_dimension_property_name(
        &self, shared_dimension_name: &String, property_name: &str
    ) -> Option<&str> {
        if let Some(aliases) = &self.aliases {
            if let Some(shared_dimensions) = &aliases.shared_dimensions {
                for shared_dimension in shared_dimensions {
                    if &shared_dimension.name == shared_dimension_name {
                        let res = shared_dimension.find_level_name(&property_name);
                        if let Some(res) = res {
                            return Some(res)
                        }
                    }
                }
            }
        }
        None
    }

    pub fn deconstruct_property_alias(
        &self, cube_name: &str, property_name: &str, schema: &Schema
    ) -> Option<&str> {
        for cube in &schema.cubes {
            if &cube.name == cube_name {
                for dimension in &cube.dimensions {

                    let property_name_opt = if dimension.is_shared {
                        self.find_shared_dimension_property_name(
                            &dimension.name, &property_name
                        )
                    } else {
                        self.find_cube_property_name(
                            &cube.name, &property_name
                        )
                    };

                    if let Some(property_name) = property_name_opt {
                        return Some(property_name)
                    }

                }
            }
        }

        None
    }
}
