pub mod types;
pub mod mgr;
mod mgr_impl;

use common;

pub fn create_metaserver_mgr(cfg: common::config::MetaServerConfig) -> Result<Box<dyn mgr::MetaServiceMgr>, Box<dyn std::error::Error>>{
    let meta_service = Box::new(mgr_impl::MetaServiceMgrImpl{});
    Ok(meta_service)
}
#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
