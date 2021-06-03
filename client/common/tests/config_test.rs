use std::path::PathBuf;
use log::{info, error};

#[test]
fn test_parse_config()->Result<(), String>{
    let pathb : PathBuf;
    match std::env::current_dir(){
        Ok(pb) => {pathb = pb;}
        Err(error) => {
            return Err(format!("failed to get current dir, err: {}", error));
        }
    }
    let dir : String;
    match pathb.as_os_str().to_str(){
        Some(p) => {dir = String::from(p);}
        None => {
            return Err(String::from("failed to convert to str"));
        }
    }
    let test_config_file = String::from(format!("{}/tests/conf/yigfs.toml", dir));
    let parse_result = common::parse_config(test_config_file);
    match parse_result {
        Ok(cfg) => {
            info!("got cfg: {:?}", cfg);
            Ok(())
        }
        Err(error) => {
            error!("failed to parse with err: {}", error);
            Err(format!{"failed to parse with err: {}", error})
        }
    }
}