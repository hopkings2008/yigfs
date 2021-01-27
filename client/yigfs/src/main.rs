mod options;

use filesystem_mgr::{FilesystemMgr, MountOptions};
use metaservice_mgr;
use common::parse_config;

fn main() {
    let opts = options::parse();
    println!("{:?}", opts);

    let parse_result = parse_config(opts.config_file_path);
    match parse_result {
        Ok(cfg) => {
            let metaservice = metaservice_mgr::create_metaserver_mgr(&cfg).unwrap();
            let filesystem = FilesystemMgr::create(metaservice);
            let mount_options = MountOptions{
                mnt: cfg.mount_config.mnt,
            };
            filesystem.mount(mount_options);
        }
        Err(error)=>{
            println!("failed to parse with err: {:}", error);
        }
    }
}