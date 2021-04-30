pub mod types;
pub mod mgr;
pub mod mgr_impl;
pub mod meta_store;
pub mod meta_op;
mod meta_thread;
mod meta_worker;
mod meta_thread_pool;

use std::sync::Arc;
use common::config::Config;
use common::runtime::Executor;


pub fn new_metaserver_mgr(cfg: &Config, exec: &Executor) -> Result<Arc<dyn mgr::MetaServiceMgr>, String>{
    let ret = mgr_impl::MetaServiceMgrImpl::new(cfg, exec);
    match ret {
        Ok(ret) => {
            return Ok(Arc::new(ret));
        }
        Err(error) => {
            return Err(format!("failed to new MetaServiceMgrImpl, err: {}", error));
        }
    }
}
