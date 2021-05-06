mod options;

use std::sync::Arc;
use filesystem_mgr::{FilesystemMgr, MountOptions};
use common::parse_config;
use common::runtime::Executor;
use common::config::Config;
use segment_mgr::segment_mgr::SegmentMgr;
use segment_mgr::leader_mgr::LeaderMgr;
use segment_mgr::heartbeat_mgr::HeartbeatMgr;
use segment_mgr::segment_sync::SegSyncer;
use metaservice_mgr::new_metaserver_mgr;
use metaservice_mgr::meta_store::MetaStore;
use io_engine::backend_store_mgr::BackendStoreMgr;
use io_engine::backend_storage::BackendStore;
use io_engine::cache_store::{CacheStore, CacheStoreConfig};
use io_engine::disk_cache_store::DiskCacheStoreFactory;
use yig_backend::backend::YigBackendFactory;

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
    let single_thread_exec = Executor::create_single_threaded();
    let meta_service = new_metaserver_mgr(&cfg, &single_thread_exec).unwrap();
    let segment_mgr = Arc::new(SegmentMgr::create(&cfg, meta_service.clone()));
    // create backend store.
    // [TODO] we should create backend store as plugin. but currently, only hardcoded.
    let mut backend_store_mgr = BackendStoreMgr::new();
    // register yig backend.
    let yig_backend_factory = YigBackendFactory::new(&single_thread_exec);
    backend_store_mgr.register(1, yig_backend_factory);
    let backend_store: Arc<dyn BackendStore>;
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
    // create cache store.
    let cache_store_config = CacheStoreConfig{
        thread_num: cfg.disk_cache_config.thread_num,
    };
    let cache_store_factory = DiskCacheStoreFactory::new();
    let cache_store: Arc<dyn CacheStore>;
    let ret = cache_store_factory.new_cache_store(
        &cache_store_config, &single_thread_exec);
    match ret {
        Ok(ret) => {
            cache_store = ret;
        }
        Err(err) => {
            println!("failed to create cache store, err: {:?}", err);
            return;
        }
    }
    // create meta_store
    let meta_store = Arc::new(MetaStore::new(
        cfg.metaserver_config.thread_num, meta_service.clone()
    ));
    // create segment_syncer
    let segment_syncer = SegSyncer::new(cache_store.clone(), backend_store.clone(), meta_store.clone());
    let syncer = Arc::new(segment_syncer);

    let leader_mgr = LeaderMgr::new(&meta_service.get_machine_id(),
    &exec, segment_mgr.clone(), cache_store.clone(), backend_store.clone());
    let mut filesystem = FilesystemMgr::create(meta_service.clone(), leader_mgr);
    let mount_options = MountOptions{
        mnt: cfg.mount_config.mnt.clone(),
    };
    // start heartbeat mgr.
    HeartbeatMgr::new(cfg.heartbeat_config.timeout, 
        syncer, 
meta_service,
        segment_mgr);
    filesystem.mount(mount_options);
}