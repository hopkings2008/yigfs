pub mod types;
pub mod mgr;
mod mgr_impl;

use common;

pub fn create_metaserver_mgr(cfg: common::config::Config) -> Result<Box<dyn mgr::MetaServiceMgr>, String>{
    let ret = mgr_impl::MetaServiceMgrImpl::new(cfg);
    match ret {
        Ok(ret) => {
            return Ok(Box::new(ret));
        }
        Err(error) => {
            return Err(format!("failed to new MetaServiceMgrImpl, err: {}", error));
        }
    }
}
