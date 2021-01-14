mod options;

use filesystem_mgr;
use metaservice_mgr;
use common;

fn main() {
    let opts = options::parse();
    println!("{:?}", opts);

    let parse_result = common::parse_config(opts.config_file_path);
    match parse_result {
        Ok(cfg) => {
            let metaservice = metaservice_mgr::create_metaserver_mgr(cfg.metaserver_config).unwrap();
            let filesystem = filesystem_mgr::FilesystemMgr::create(metaservice);
            let mount_options = filesystem_mgr::MountOptions{
                mnt: cfg.mount_config.mnt,
            };
            //filesystem.mount(mount_options);
        }
        Err(error)=>{
            println!("failed to parse with err: {:}", error);
        }
    }
}