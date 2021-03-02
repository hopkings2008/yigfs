mod options;

use filesystem_mgr::{FilesystemMgr, MountOptions};
use common::parse_config;
use common::runtime::Executor;
use common::config::Config;
use segment_mgr::segment_mgr::SegmentMgr;
use metaservice_mgr::new_metaserver_mgr;

fn main() {
    let opts = options::parse();
    println!("{:?}", opts);

    let cfg: Config;
    let parse_result = parse_config(opts.config_file_path);
    match parse_result {
        Ok(ret) => {
            cfg = ret;
        }
        Err(error)=>{
            println!("failed to parse with err: {:}", error);
            return;
        }
    }

    let mut dirs:Vec<String> = Vec::new();
    dirs.push(String::from("/data/yigfs"));
    let exec = Executor::create();
    let meta_service = new_metaserver_mgr(&cfg, &exec).unwrap();
    let segment_mgr = SegmentMgr::create(dirs, &meta_service, &exec);
    let filesystem = FilesystemMgr::create(&meta_service, &segment_mgr);
    let mount_options = MountOptions{
        mnt: cfg.mount_config.mnt.clone(),
    };
    filesystem.mount(mount_options);
}