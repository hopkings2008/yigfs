mod options;

use std::rc::Rc;
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

    let exec = Executor::create();
    let meta_service = new_metaserver_mgr(&cfg, &exec).unwrap();
    let segment_mgr = Rc::new(SegmentMgr::create(&cfg, meta_service.clone()));
    let filesystem = FilesystemMgr::create(meta_service.clone(), segment_mgr.clone(), &exec);
    let mount_options = MountOptions{
        mnt: cfg.mount_config.mnt.clone(),
    };
    filesystem.mount(mount_options);
}