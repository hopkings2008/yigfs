pub mod config;
pub mod json;
pub mod http_client;
pub mod time;
pub mod error;
pub mod uuid;
pub mod defer;
pub mod runtime;
pub mod thread;
pub mod numbers;
use log::{info, error};

pub fn parse_config(path: String)->Result<config::Config, Box<dyn std::error::Error>>{
    let results = std::fs::read_to_string(path);
    match results {
        Ok(content) => {
            let cfg = toml::from_str(content.as_str())?;
            info!("cfg: {:?}", cfg);
            return Ok(cfg);
        }
        Err(error) => {
            error!("failed to parse, err: {:?}", error);
            return Err(Box::new(error));
        }
    }
}