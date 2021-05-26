use std::path::PathBuf;
use log::{error, info};
use log4rs;

#[test]
fn test_config_log()->Result<(), String> {
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
    let test_config_log = String::from(format!("{}/tests/conf/yigfs_log.yaml", dir));
    // config log
    let config_log_resp = log4rs::init_file(test_config_log, Default::default());
    match config_log_resp {
        Ok(_) => {
            info!("Succeed to config log!");
            return Ok(())
        }
        Err(error) => {
            error!("Failed to config log!, err: {}", error);
            return Err(format!("failed to config log err: {}", error));
        } 
    }
}
