pub mod config;
pub mod json;
pub mod http_client;
pub mod time;

pub fn parse_config(path: String)->Result<config::Config, Box<dyn std::error::Error>>{
    let results = std::fs::read_to_string(path);
    match results {
        Ok(content) => {
            let cfg = toml::from_str(content.as_str())?;
            println!("cfg: {:?}", cfg);
            return Ok(cfg);
        }
        Err(error) => {
            println!("failed to parse, err: {:?}", error);
            return Err(Box::new(error));
        }
    }
}