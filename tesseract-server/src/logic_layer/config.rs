use failure::{Error, format_err};

use serde_derive::Deserialize;
use serde_json;


#[derive(Debug, Clone, Deserialize)]
pub struct LogicLayerConfig {
    pub aliases: Option<AliasConfig>,
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


/// Reads Logic Layer Config JSON file.
pub fn read_config(config_path: &String) -> Result<LogicLayerConfig, Error> {
    let config_str = std::fs::read_to_string(&config_path)
        .map_err(|_| format_err!("Logic layer config file not found at {}", config_path))?;

    match serde_json::from_str::<LogicLayerConfig>(&config_str) {
        Ok(config) => return Ok(config),
        Err(err) => {
            return Err(format_err!("Unable to read logic layer config at {}: {}", config_path, err))
        }
    };
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
}
