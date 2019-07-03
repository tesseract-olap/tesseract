use failure::{Error, format_err};
use std::env;

pub fn get_bool_env_var(var_name: &str, default_val: bool) -> bool {
    let env_var_result = env::var(var_name)
        .map_err(|_| format_err!(""))
        .and_then(|d| {
             d.parse::<bool>()
            .map_err(|_| format_err!("could not parse bool from env_var {}", var_name))
        });
    default_val || Result::is_ok(&env_var_result)
}
