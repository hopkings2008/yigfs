pub mod mgr;
mod mgr_impl;

pub fn CreateMetaSerivceMgr() -> Result<Box<dyn mgr::MetaServiceMgr>, Box<dyn std::error::Error>>{
    let metaService = Box::new(mgr_impl::MetaServiceMgrImpl{});
    Ok(metaService)
}
#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
