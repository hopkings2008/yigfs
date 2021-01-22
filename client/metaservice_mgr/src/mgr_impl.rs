#[path = "./message.rs"]
mod message;

use crate::mgr;
use crate::types::DirEntry;
use crate::types::FileAttr;
use common::http_client;
use common::http_client::RespText;
use common::config;
use common::json;
use message::{MsgFileAttr, ReqDirFileAttr, ReqFileAttr, ReqReadDir, RespDirFileAttr, RespFileAttr, RespReadDir};
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

    fn read_dir_file_attr(&self, ino: u64, name: &String) -> Result<Vec<FileAttr>, String>{
        let ret = self.read_dir_file_attr(ino, name);
        let mut v = Vec::new();
        match ret {
            Ok(ret) => {
                for attr in ret {
                    let file_attr = self.to_file_attr(&attr);
                    v.push(file_attr);
                }
                return Ok(v);
            }
            Err(error) => {
                return Err(error);
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

    fn to_file_attr(&self, msg_attr: &MsgFileAttr) -> FileAttr {
        FileAttr {
            ino: msg_attr.ino,
            generation: msg_attr.generation,
            size: msg_attr.size,
            blocks: msg_attr.blocks,
            atime: msg_attr.atime,
            mtime: msg_attr.mtime,
            ctime: msg_attr.ctime,
            kind: msg_attr.kind.into(),
            perm: msg_attr.perm,
            nlink: msg_attr.nlink,
            uid: msg_attr.uid,
            gid: msg_attr.gid,
            rdev: msg_attr.rdev,
            flags: msg_attr.flags,
        }
    }

    fn read_file_attr(&self, ino: u64) -> Result<MsgFileAttr, String> {
        let req_file_attr = ReqFileAttr{
            region: self.region.clone(),
            bucket: self.bucket.clone(),
            ino: ino,
        };
        let ret = json::encode_to_str::<ReqFileAttr>(&req_file_attr);
        let req_body : String;
        match ret {
            Ok(body) => {
                req_body = body;
            }
            Err(error) => {
                return Err(format!("failed to encode req_file_attr: {:?}, err: {}", req_file_attr, error));
            }
        }
        let resp : RespText;
        let url = format!("{}/v1/file/attr", self.meta_server_url);
        let ret = self.http_client.get(&url, &req_body);
        match ret {
            Ok(ret) => {
                resp = ret;
            }
            Err(error) => {
                return Err(error);
            }
        }
        if resp.status >= 300 {
            return Err(format!("failed to read_file_attr from {}, for ino: {}, err: {}",
        url, ino, resp.body));
        }
        let resp_attr: RespFileAttr;
        let ret = json::decode_from_str::<RespFileAttr>(&resp.body);
        match ret {
            Ok(ret) => {
                resp_attr = ret;
            }
            Err(error) => {
                return Err(error);
            }
        }
        if resp_attr.result.err_code != 0 {
            return Err(format!("failed to read_file_attr for ino: {}, err: {}",
        ino, resp_attr.result.err_msg));
        }

        return Ok(resp_attr.attr);
    }

    fn read_dir_file_attr(&self, ino: u64, name: &String) -> Result<Vec<MsgFileAttr>, String>{
        let req_dir_file_attr = ReqDirFileAttr{
            region: self.region.clone(),
            bucket: self.bucket.clone(),
            ino: ino,
            name: String::from(name),
        };
        let ret = json::encode_to_str::<ReqDirFileAttr>(&req_dir_file_attr);
        let req_child_file_attr_json: String;
        match ret {
            Ok(body) => {
                req_child_file_attr_json = body;
            }
            Err(error) => {
                return Err(error);
            }
        }
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
        let attrs : RespDirFileAttr;
        let ret = json::decode_from_str::<RespDirFileAttr>(&resp_text.body);
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
        let req_read_dir_json: String;
        match ret {
            Ok(ret) => {
                //send the req to meta server
                req_read_dir_json = ret;
            }
            Err(error) => {
                return Err(format!("faied to convert {:?} to json, err: {}", req_read_dir, error));
            }
        }

        let resp_body :String;
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