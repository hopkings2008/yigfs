#[path = "./message.rs"]
mod message;

use crate::mgr;
use crate::types::DirEntry;
use common::http_client;
use common::http_client::RespText;
use common::config;
use common::json;
use message::{MsgFileAttr, ReqChildFileAttr, ReqReadDir, RespChildFileAttr, RespReadDir};
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

    fn read_child_file_attr(&self, ino: u64, name: String) -> Result<Vec<MsgFileAttr>, String>{
        let req_child_file_attr = ReqChildFileAttr{
            region: self.region.clone(),
            bucket: self.bucket.clone(),
            ino: ino,
            name: name.clone(),
        };
        let ret = json::encode_to_str(&req_child_file_attr);
        let mut req_child_file_attr_json: String;
        match ret {
            Ok(body) => {
                req_child_file_attr_json = body;
            }
            Err(error) => {
                return Err(error);
            }
        }
        let mut resp_body : String;
        let resp_text : RespText;
        let url = format!("{}/v1/dir/file/attr", self.meta_server_url);
        let ret = self.http_client.get(&url, &req_child_file_attr_json);
        match ret {
            Ok(resp) => {
                resp_text = resp;
            }
            Err(error) => {
                return Err(error);
            }
        }
        if resp_text.status >= 300 {
            return Err(format!("failed to get child file attr from url {}, err: {}", url, resp_text.body));
        }
        let attrs : RespChildFileAttr;
        let ret = json::decode_from_str::<RespChildFileAttr>(&resp_text.body);
        match ret {
            Ok(attr) => {
                attrs = attr;
            }
            Err(error) => {
                return Err(error);
            }
        }
        if attrs.result.err_code != 0 {
            return Err(format!("failed to get child file attrs for ino: {}, name: {}, err: {}", 
            ino, name, attrs.result.err_msg));
        }
        return Ok(attrs.attrs);
    }

    fn read_dir_meta(&self, ino: u64, offset: i64) -> Result<Box<RespReadDir>, String>{
        let req_read_dir = ReqReadDir{
            region: self.region.clone(),
            bucket:self.bucket.clone(),
            ino: ino,
            offset: offset,
        };
        let ret = serde_json::to_string(&req_read_dir);
        let mut req_read_dir_json: String;
        match ret {
            Ok(ret) => {
                //send the req to meta server
                req_read_dir_json = ret;
            }
            Err(error) => {
                return Err(format!("faied to convert {:?} to json, err: {}", req_read_dir, error));
            }
        }

        let mut resp_body :String;
        let url = format!("{}/v1/dir/files", self.meta_server_url);
        let ret = self.http_client.get(&url, &req_read_dir_json);
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
        
        let resp_read_dir = json::decode_from_str::<RespReadDir>(&resp_body);
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