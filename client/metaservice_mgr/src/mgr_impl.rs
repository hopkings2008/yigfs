#[path = "./message.rs"]
mod message;

use crate::mgr;
use crate::types::DirEntry;
use common::http_client;
use common::config;
use common::json;
use message::{ReqReadDir, RespReadDir};
pub struct MetaServiceMgrImpl{
    http_client: Box<http_client::HttpClient>,
    meta_server_url: String,
    region: String,
    bucket: String,
}

impl mgr::MetaServiceMgr for MetaServiceMgrImpl{
    fn read_dir(&self, ino: u64, offset: i64)->Result<Vec<DirEntry>, String>{
        let mut entrys = Vec::new();
        let ret = self.read_dir_meta(ino, offset);
        match ret {
            Ok(dirs) => {
                if dirs.result.err_code != 0 {
                    return Err(dirs.result.err_msg);
                }
                for i in dirs.files {
                    let entry = DirEntry{
                        ino: i.ino,
                        file_type: i.dir_entry_type.into(),
                        name: i.name,
                    };
                    entrys.push(entry);
                }
                return Ok(entrys);
            }
            Err(error) => {
                return Err(format!("failed to read meta for ino: {}, offset: {}, err: {}",
            ino, offset, error));
            }
        }
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
        })
    }

    fn read_dir_meta(&self, ino: u64, offset: i64) -> Result<Box<RespReadDir>, String>{
        let req_read_dir = ReqReadDir{
            region: self.region.clone(),
            bucket:self.bucket.clone(),
            ino: ino,
            offset: offset,
        };
        let ret = serde_json::to_string(&req_read_dir);
        let mut req_read_dir_json = String::new();
        match ret {
            Ok(ret) => {
                //send the req to meta server
                req_read_dir_json = ret;
            }
            Err(error) => {
                return Err(format!("faied to convert {:?} to json, err: {}", req_read_dir, error));
            }
        }

        let mut resp_body = String::new();
        let url = format!("{}/v1/dir/files", self.meta_server_url);
        let ret = self.http_client.get(&url, req_read_dir_json);
        match ret {
            Ok(text) => {
                if text.status >= 300 {
                    return Err(format!("got resp {}", text.status));
                }
                resp_body = text.body;
            }
            Err(error) => {
                return Err(format!("failed to get response for {}, err: {}", url, error));
            }
        }
        
        let resp_read_dir = json::decode_from_str(&resp_body);
        match resp_read_dir {
            Ok(resp_read_dir) => {
                return Ok(Box::new(resp_read_dir));
            }
            Err(error) => {
                return Err(format!("failed to decode from {}, err: {}", resp_body, error));
            }
        }
    }
}