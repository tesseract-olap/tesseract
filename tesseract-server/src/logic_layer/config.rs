use failure::{Error, format_err};

use serde_derive::Deserialize;
use serde_json;


#[derive(Debug, Clone, Deserialize)]
pub struct LogicLayerConfig {
    pub aliases: Option<Vec<AliasConfig>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AliasConfig {
    pub name: String,
    pub cube: String,
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
