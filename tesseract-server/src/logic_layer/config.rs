use failure::{Error, format_err};
use std::collections::HashSet;

use serde_derive::Deserialize;
use serde_json;


#[derive(Debug, Clone, Deserialize)]
pub struct LogicLayerConfig {
    pub aliases: Option<AliasConfig>,
    pub named_sets: Option<Vec<NamedSetsConfig>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AliasConfig {
    pub cubes: Option<Vec<CubeAliasConfig>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CubeAliasConfig {
    pub name: String,
    pub alternatives: Vec<String>
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
}
