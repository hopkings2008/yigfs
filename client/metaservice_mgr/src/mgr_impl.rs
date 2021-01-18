use crate::mgr;
use common::http_client;
use common::config;
pub struct MetaServiceMgrImpl{
    http_client: Box<http_client::HttpClient>,
    meta_server_url: String,
    region: String,
    bucket: String,
    ak: String,
    sk: String,
}

impl mgr::MetaServiceMgr for MetaServiceMgrImpl{
    fn read_dir(&self, ino: u64, offset: i64)->Result<Vec<mgr::types::DirEntry>, String>{
        let entrys = Vec::new();
        return Ok(entrys);
    }
}

impl MetaServiceMgrImpl {
    pub fn new(meta_cfg: config::Config) -> Result<MetaServiceMgrImpl, String> {
        let http_client = Box::new(http_client::HttpClient::new(3));
        Ok(MetaServiceMgrImpl{
            http_client: http_client,
            meta_server_url: meta_cfg.metaserver_config.meta_server,
            region: meta_cfg.s3_config.region,
            bucket: meta_cfg.s3_config.bucket,
            ak: meta_cfg.s3_config.ak,
            sk: meta_cfg.s3_config.sk,
        })
    }
}