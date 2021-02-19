mod options;

use filesystem_mgr::{FilesystemMgr, MountOptions};
use metaservice_mgr::{self, types::Segment};
use common::parse_config;
use segment_mgr::segment_mgr::SegmentMgr;

fn main() {
    let opts = options::parse();
    println!("{:?}", opts);

    let parse_result = parse_config(opts.config_file_path);
    match parse_result {
        Ok(cfg) => {
            let meta_service = metaservice_mgr::create_metaserver_mgr(&cfg).unwrap();
            let segment_mgr = SegmentMgr::create(&meta_service);
            let filesystem = FilesystemMgr::create(&meta_service, &segment_mgr);
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