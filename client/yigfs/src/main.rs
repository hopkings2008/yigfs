mod options;

use std::rc::Rc;
use filesystem_mgr::{FilesystemMgr, MountOptions};
use common::parse_config;
use common::runtime::Executor;
use common::config::Config;
use segment_mgr::segment_mgr::SegmentMgr;
use segment_mgr::leader_mgr::LeaderMgr;
use metaservice_mgr::new_metaserver_mgr;
use io_engine::backend_store_mgr::BackendStoreMgr;
use io_engine::backend_storage::BackendStore;

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
    // create backend store.
    // [TODO] we should create backend store as plugin. but currently, only hardcoded.
    let backend_store_mgr = BackendStoreMgr::new();
    let backend_store: Box<dyn BackendStore>;
    let ret = 
    backend_store_mgr.get_backend_store(cfg.backend_store_config.backend_type, &cfg.backend_store_config.settings);
    match ret {
        Ok(ret) => {
            backend_store = ret;
        }
        Err(err) => {
            println!("failed to create backend store, err: {:?}", err);
            return;
        }
    }
    let leader_mgr = LeaderMgr::new(&meta_service.get_machine_id(),
    cfg.disk_cache_config.thread_num, &exec, segment_mgr.clone(), backend_store);
    let mut filesystem = FilesystemMgr::create(meta_service.clone(), leader_mgr);
    let mount_options = MountOptions{
        mnt: cfg.mount_config.mnt.clone(),
    };
    filesystem.mount(mount_options);
}