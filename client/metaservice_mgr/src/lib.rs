pub mod types;
pub mod mgr;
pub mod mgr_impl;

use common::config::Config;
use common::runtime::Executor;


pub fn new_metaserver_mgr(cfg: &Config, exec: &Executor) -> Result<Box<dyn mgr::MetaServiceMgr>, String>{
    let ret = mgr_impl::MetaServiceMgrImpl::new(cfg, exec);
    match ret {
        Ok(ret) => {
            return Ok(Box::new(ret));
        }
        Err(error) => {
            return Err(format!("failed to new MetaServiceMgrImpl, err: {}", error));
        }
    }
}
