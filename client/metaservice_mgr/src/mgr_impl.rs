extern crate tokio;
#[path="./message.rs"]
mod message;

use std::sync::Arc;
use crate::{mgr, types::{Block, FileLeader, NewFileInfo, Segment, SetFileAttr}};
use crate::types::DirEntry;
use crate::types::FileAttr;
use crate::types::{HeartbeatResult, HeartbeatUploadSeg};
use common::http_client;
use common::http_client::RespText;
use common::config::Config;
use common::json;
use common::error::Errno;
use common::http_client::HttpMethod;
use common::runtime::Executor;
use message::{MsgBlock, MsgFileAttr, MsgSegment, MsgSetFileAttr, ReqAddBlock, ReqDirFileAttr, ReqFileAttr, ReqFileCreate, ReqFileLeader, ReqGetSegments, ReqMount, ReqReadDir, ReqSetFileAttr, ReqUploadSegment, RespAddBock, RespDirFileAttr, RespFileAttr, RespFileCreate, RespFileLeader, RespGetSegments, RespHeartbeat, RespReadDir, RespSetFileAttr, RespUploadSegment};

use self::message::{MsgSegmentOffset, ReqHeartbeat, ReqUpdateSegments, RespUpdateSegments};
pub struct MetaServiceMgrImpl{
    http_client: Arc<http_client::HttpClient>,
    meta_server_url: String,
    region: String,
    bucket: String,
    zone: String,
    machine: String,
    exec: Executor,
}

impl mgr::MetaServiceMgr for MetaServiceMgrImpl{
    fn mount(&self, uid: u32, gid: u32) -> Result<(), Errno>{
        let req = ReqMount{
            region: self.region.clone(),
            bucket: self.bucket.clone(),
            zone: self.zone.clone(),
            machine: self.machine.clone(),
            uid: uid,
            gid: gid,
        };

        let req_json: String;
        let ret = common::json::encode_to_str::<ReqMount>(&req);
        match ret {
            Ok(ret) => {
                req_json = ret;
            }
            Err(error) => {
                println!("failed to encode {:?}, err: {}", req, error);
                return Err(Errno::Eintr);
            }
        }

        let url = format!("{}/v1/dir", self.meta_server_url);
        let resp : RespText;
        let ret = self.exec.get_runtime().block_on(self.http_client.request(&url, &req_json.as_bytes(), &HttpMethod::Put, false));
        match ret {
            Ok(ret) => {
                resp = ret;
            }
            Err(error) => {
                println!("failed to mount region: {}, bucket: {}, err: {}",
                self.region, self.bucket, error);
                return Err(Errno::Eintr);
            }
        }
        if resp.status >= 300 {
            println!("failed to mount region: {}, bucket: {}, got status: {}, body: {}",
            self.region, self.bucket, resp.status, resp.body);
            return Err(Errno::Eintr);
        }
        Ok(())
    }
    fn read_dir(&self, ino: u64, offset: i64)->Result<Vec<DirEntry>, Errno>{
        let mut entrys = Vec::new();
        let ret = self.read_dir_files(ino, offset);
        match ret {
            Ok(dirs) => {
                if dirs.result.err_code != 0 {
                    if dirs.result.err_code == 40003 {
                        println!("no files found in bucket {} with ino: {}, offset: {}", self.bucket, ino, offset);
                        return Err(Errno::Enoent);
                    }
                    println!("got error when read_dir_files for ino: {}, offset: {}, err: {}",
                    ino, offset, dirs.result.err_msg);
                    return Err(Errno::Eintr);
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
                println!("failed to read meta for ino: {}, offset: {}, err: {}",
                ino, offset, error);
                return Err(Errno::Eintr);
            }
        }
    }

    fn read_file_attr(&self, ino: u64) -> Result<FileAttr, Errno>{
        let attr : MsgFileAttr;
        let ret = self.read_file_attr(ino);
        match ret {
            Ok(ret) => {
                attr = ret;
            }
            Err(error) => {
                println!("failed to read_file_attr for ino: {}, err: {}", ino, error);
                return Err(Errno::Eintr);
            }
        }

        let file_attr = self.to_file_attr(&attr);
        Ok(file_attr)
    }

    fn set_file_attr(&self, attr: &SetFileAttr) -> Result<FileAttr, Errno> {
        let req = ReqSetFileAttr {
            region: self.region.clone(),
            bucket: self.bucket.clone(),
            attr: MsgSetFileAttr{
                ino: attr.ino,
                size: attr.size,
                atime: attr.atime,
                mtime: attr.mtime,
                ctime: attr.ctime,
                perm: attr.perm,
                uid: attr.uid,
                gid: attr.gid,
            },
        };
        let req_str: String;
        let ret = json::encode_to_str::<ReqSetFileAttr>(&req);
        match ret {
            Ok(ret) => {
                req_str = ret;
            }
            Err(err) => {
                println!("failed to encode_to_str for attr: {:?}, err: {}", req, err);
                return Err(Errno::Eintr);
            }
        }

        let url = format!("{}/v1/file/attr", self.meta_server_url);
        let resp_text : RespText;
        let ret = self.exec.get_runtime().block_on(self.http_client.request(&url, &req_str.as_bytes(), &HttpMethod::Put, false));
        match ret {
            Ok(ret) => {
                resp_text = ret;
            }
            Err(err) => {
                println!("failed to set_file_attr: {}, err: {}", req_str, err);
                return Err(Errno::Eintr);
            }
        }

        if resp_text.status >= 300 {
            println!("failed to set_file_attr: {}, got status: {}, resp body: {}", 
            req_str, resp_text.status, resp_text.body);
            return Err(Errno::Eintr);
        }

        let resp : RespSetFileAttr;
        let ret = json::decode_from_str::<RespSetFileAttr>(&resp_text.body);
        match ret {
            Ok(ret) => {
                resp = ret;
            }
            Err(err) => {
                println!("got invalid resp {} for set_file_attr: {}, err: {}", resp_text.body, req_str, err);
                return Err(Errno::Eintr);
            }
        }

        if resp.result.err_code != 0 {
            println!("failed to set_file_attr for {}, err_code: {}, err_msg: {}",
            req_str, resp.result.err_code, resp.result.err_msg);
            return Err(Errno::Eintr);
        }

        Ok(self.to_file_attr(&resp.attr))
    }

    fn read_dir_file_attr(&self, ino: u64, name: &String) -> Result<FileAttr, Errno>{
        let ret = self.read_dir_file_attr(ino, name);
        match ret {
            Ok(ret) => {
                let file_attr = self.to_file_attr(&ret);
                return Ok(file_attr);
            }
            Err(error) => {
                println!("failed to read_dir_file_attr for ino: {}, name: {}, err: {}", ino, name, error);
                return Err(Errno::Eintr);
            }
        }
    }

    fn get_file_leader(&self, ino: u64) -> Result<FileLeader, Errno>{
        let req_file_leader = ReqFileLeader{
            region: self.region.clone(),
            bucket: self.bucket.clone(),
            zone: self.zone.clone(),
            machine: self.machine.clone(),
            ino: ino,
        };
        let body: String;
        let ret = json::encode_to_str::<ReqFileLeader>(&req_file_leader);
        match ret {
            Ok(ret) => {
                body = ret;
            }
            Err(error) => {
                println!("failed to encode req_file_leader: {:?}, err: {}", req_file_leader, error);
                return Err(Errno::Eintr);
            }
        }
        let url = format!("{}/v1/file/leader", self.meta_server_url);
        let resp : RespText;
        let ret = self.exec.get_runtime().block_on(self.http_client.request(&url, &body.as_bytes(), &HttpMethod::Get, false));
        match ret {
            Ok(ret) => {
                resp = ret;
            }
            Err(error) => {
                println!("failed to get file_leader, req: {}, err: {}", body, error);
                return Err(Errno::Eintr);
            }
        }
        if resp.status >= 300 {
            println!("got status {} for file_leader, req: {}, resp: {}", resp.status, body, resp.body);
            return Err(Errno::Eintr);
        }
        let resp_leader : RespFileLeader;
        let ret = json::decode_from_str::<RespFileLeader>(&resp.body);
        match ret {
            Ok(ret) => {
                resp_leader = ret;
            }
            Err(error) => {
                println!("failed to decode file_leader from {}, err: {}", resp.body, error);
                return Err(Errno::Eintr);
            }
        }
        if resp_leader.result.err_code != 0 {
            println!("failed to get file_leader for {}, err_code: {}, err_msg: {}", 
            body, resp_leader.result.err_code, resp_leader.result.err_msg);
            return Err(Errno::Eintr);
        }
        Ok(FileLeader{
            zone: resp_leader.leader_info.zone,
            leader: resp_leader.leader_info.leader,
            ino: ino,
        })
    }

    fn new_ino_leader(&self, parent: u64, name: &String, uid: u32, gid: u32, perm: u32) -> Result<NewFileInfo, Errno> {
        let req_file_create = ReqFileCreate{
            zone: self.zone.clone(),
            machine: self.machine.clone(),
            region: self.region.clone(),
            bucket: self.bucket.clone(),
            ino: parent,
            name: name.clone(),
            uid: uid,
            gid: gid,
            perm: perm,
        };
        let body : String;
        let ret = json::encode_to_str::<ReqFileCreate>(&req_file_create);
        match ret {
            Ok(ret) => {
                body = ret;
            }
            Err(error) => {
                println!("failed to encode req_file_create: {:?}, err: {}", req_file_create, error);
                return Err(Errno::Eintr);
            }
        }
        let url = format!("{}/v1/dir/file", self.meta_server_url);
        let resp: RespText;
        let ret = self.exec.get_runtime().block_on(self.http_client.request(&url, &body.as_bytes(), &HttpMethod::Put, false));
        match ret {
            Ok(ret) => {
                resp = ret;
            }
            Err(error) => {
                println!("failed to new_ino_leader for {}, err: {}", body, error);
                return Err(Errno::Eintr);
            }
        }
        if resp.status >= 300 {
            println!("got status {} for new_ino_leader {}, resp: {}", resp.status, body, resp.body);
            return Err(Errno::Eintr);
        }
        let resp_file_created: RespFileCreate;
        let ret = json::decode_from_str::<RespFileCreate>(&resp.body);
        match ret {
            Ok(ret) => {
                resp_file_created = ret;
            }
            Err(error) => {
                println!("failed to decode {} for new_ino_leader: {}, err: {}", resp.body, body, error);
                return Err(Errno::Eintr);
            }
        }
        if resp_file_created.result.err_code != 0 {
            println!("failed to new_io_leader for {}, err_code: {}, err_msg: {}", 
            body, resp_file_created.result.err_code, resp_file_created.result.err_msg);
            return Err(Errno::Eintr);
        }

        Ok(NewFileInfo{
            leader_info: FileLeader{
                zone: resp_file_created.leader_info.zone,
                leader: resp_file_created.leader_info.leader,
                ino: resp_file_created.file_info.ino,
            },
            attr: self.to_file_attr(&resp_file_created.file_info),
        })
    }

    fn get_file_segments(&self, ino: u64, offset: Option<u64>, size: Option<i64>) -> Result<Vec<Segment>, Errno>{
        let req_get_segments = ReqGetSegments{
            zone: self.zone.clone(),
            region: self.region.clone(),
            bucket: self.bucket.clone(),
            machine: self.machine.clone(),
            ino: ino,
            generation: 0,
            offset: offset,
            size: size,
        };
        let body: String;
        let ret = json::encode_to_str::<ReqGetSegments>(&req_get_segments);
        match ret {
            Ok(ret) => {
                body = ret;
            }
            Err(err) => {
                println!("failed to encode {:?}, err: {}", req_get_segments, err);
                return Err(Errno::Eintr);
            }
        }
        let url = format!("{}/v1/file/segments", self.meta_server_url);
        let resp_text: RespText;
        let ret = self.exec.get_runtime().block_on(self.http_client.request(&url, &body.as_bytes(), &HttpMethod::Get, false));
        match ret  {
            Ok(ret) => {
                resp_text = ret;
            }
            Err(err) => {
                println!("failed to send {} to get_file_segments, err: {}", body, err);
                return Err(Errno::Eintr);
            }
        }
        if resp_text.status >= 300 {
            println!("get_file_segments failed with status_code: {}, resp: {}", resp_text.status, resp_text.body);
            return Err(Errno::Eintr);
        }
        let resp : RespGetSegments;
        let ret = json::decode_from_str::<RespGetSegments>(&resp_text.body);
        match ret {
            Ok(ret) => {
                resp = ret;
            }
            Err(err) => {
                println!("got invalid response: {} for get_file_segments, err: {}", resp_text.body, err);
                return Err(Errno::Eintr);
            }
        }
        if resp.result.err_code != 0 {
            println!("failed to get_file_segments for {}, err_code: {}, err_msg: {}",
            body, resp.result.err_code, resp.result.err_msg);
            return Err(Errno::Eintr);
        }
        let mut segments: Vec<Segment> = Vec::new();
        for s in resp.segments {
            let mut segment : Segment = Default::default();
            segment.seg_id0 = s.seg_id0;
            segment.seg_id1 = s.seg_id1;
            segment.capacity = s.capacity;
            segment.backend_size = s.backend_size;
            segment.size = s.size;
            segment.leader = s.leader;
            for b in s.blocks {
                let block = Block{
                    offset: b.offset,
                    seg_start_addr: b.seg_start_addr,
                    seg_end_addr: b.seg_end_addr,
                    size: b.size,
                };
                segment.blocks.push(block);
            }
            segments.push(segment);
        }
        Ok(segments)
    }

    fn get_machine_id(&self) -> String {
        self.machine.clone()
    }

    fn add_file_block(&self, ino: u64, seg: &Segment) -> Errno {
        let mut s = MsgSegment{
            seg_id0: seg.seg_id0,
            seg_id1: seg.seg_id1,
            capacity: seg.capacity,
            size: seg.size,
            backend_size: seg.backend_size,
            leader: seg.leader.clone(),
            blocks: Vec::new(),
        };
        for b in &seg.blocks {
            let bl = MsgBlock {
                offset: b.offset,
                seg_start_addr: b.seg_start_addr,
                seg_end_addr: b.seg_end_addr,
                size: b.size,
            };
            s.blocks.push(bl);
        }

        let req = ReqAddBlock {
            region: self.region.clone(),
            bucket: self.bucket.clone(),
            zone: self.zone.clone(),
            machine: self.machine.clone(),
            ino: ino,
            generation: 0,
            segment: s,
        };

        let body: String;
        let ret = json::encode_to_str::<ReqAddBlock>(&req);
        match ret {
            Ok(ret) => {
                body = ret;
            }
            Err(err) => {
                println!("add_file_block: failed to encode req: {:?}, err: {}", req, err);
                return Errno::Eintr;
            }
        }

        let url = format!("{}/v1/file/block", self.meta_server_url);
        let resp_text: RespText;
        let ret = self.exec.get_runtime().block_on(self.http_client.request(&url, &body.as_bytes(), &HttpMethod::Put, false));
        match ret {
            Ok(ret) => {
                resp_text = ret;
            }
            Err(err) => {
                println!("add_file_block: failed to send req to {} with body: {}, err: {}",
                url, body, err);
                return Errno::Eintr;
            }
        }

        if resp_text.status >=300 {
            println!("add_file_block: failed to add block for {}, got status: {}",
            body, resp_text.status);
            return Errno::Eintr;
        }

        let resp : RespAddBock;
        let ret = json::decode_from_str::<RespAddBock>(&resp_text.body);
        match ret {
            Ok(ret) => {
                resp = ret;
            }
            Err(err) => {
                println!("add_file_block: failed to decode body: {}, err: {}", resp_text.body, err);
                return Errno::Eintr;
            }
        }

        if resp.result.err_code != 0 {
            println!("add_file_block: failed to add file block for {}, err: {}", body, resp.result.err_msg);
            return Errno::Eintr;
        }
        return Errno::Esucc;
    }

    fn update_file_segments(&self, ino: u64, segs: &Vec<Segment>) -> Errno{
        let mut vs: Vec<MsgSegment> = Vec::new();
        for s in segs {
            vs.push(MetaServiceMgrImpl::to_msg_segment(s));
        }

        let req_update_seg = ReqUpdateSegments {
            region: self.region.clone(),
            bucket: self.bucket.clone(),
            zone: self.zone.clone(),
            ino: ino,
            generation: 0,
            segments: vs,
        };

        let body: String;
        let ret = json::encode_to_str::<ReqUpdateSegments>(&req_update_seg);
        match ret {
            Ok(ret) => {
                body = ret;
            }
            Err(err) => {
                println!("update_file_segments: failed to encode req: {:?}, err: {}", req_update_seg, err);
                return Errno::Eintr;
            }
        }

        let url = format!("{}/v1/file/segments", self.meta_server_url);
        let resp_text: RespText;
        let ret = self.exec.get_runtime().block_on(self.http_client.request(&url, &body.as_bytes(), &HttpMethod::Put, false));
        match ret {
            Ok(ret) => {
                resp_text = ret;
            }
            Err(err) => {
                println!("update_file_segments: failed to send req to {} with body: {}, err: {}",
                url, body, err);
                return Errno::Eintr;
            }
        }

        if resp_text.status >=300 {
            println!("update_file_segments: failed to add block for {}, got status: {}",
            body, resp_text.status);
            return Errno::Eintr;
        }

        let resp : RespUpdateSegments;
        let ret = json::decode_from_str::<RespUpdateSegments>(&resp_text.body);
        match ret {
            Ok(ret) => {
                resp = ret;
            }
            Err(err) => {
                println!("update_file_segments: failed to decode body: {}, err: {}", resp_text.body, err);
                return Errno::Eintr;
            }
        }

        if resp.result.err_code != 0 {
            println!("update_file_segments: failed to add file block for {}, err: {}", body, resp.result.err_msg);
            return Errno::Eintr;
        }

        return Errno::Esucc;
    }

    fn upload_segment(&self, id0: u64, id1: u64, next_offset: u64) -> Errno{
        let req_upload_seg = ReqUploadSegment{
            region: self.region.clone(),
            bucket: self.bucket.clone(),
            zone: self.zone.clone(),
            machine: self.machine.clone(),
            segment: MsgSegmentOffset{
                seg_id0: id0,
                seg_id1: id1,
                backend_size: next_offset,
            },
        };
        let req_body: String;
        let ret = json::encode_to_str::<ReqUploadSegment>(&req_upload_seg);
        match ret {
            Ok(ret) => {
                req_body = ret;
            }
            Err(ret) => {
                println!("upload_segment: failed to encode to json for id0: {}, id1: {}, next_offset: {}, err: {}",
            id0, id1, next_offset, ret);
                return Errno::Eintr;
            }
        }

        let url = format!("{}/v1/segment/block", self.meta_server_url);
        let resp_body: RespText;
        let ret = self.exec.get_runtime().block_on(self.http_client.request(
            &url, req_body.as_bytes(), &HttpMethod::Put, false));
        match ret{
            Ok(ret) => {
                resp_body = ret;
            }
            Err(err) => {
                println!("upload_segment: failed to send req: {}, err: {}", req_body, err);
                return Errno::Eintr;
            }
        }
        if resp_body.status >= 300 {
            println!("upload_segment: got resp status: {}, resp_body: {} for req: {}",
            resp_body.status, resp_body.body, req_body);
            return Errno::Eintr;
        }

        let resp: RespUploadSegment;
        let ret = json::decode_from_str::<RespUploadSegment>(&resp_body.body);
        match ret {
            Ok(ret) => {
                resp = ret;
            }
            Err(err) => {
                println!("upload_segment: failed to decode resp: {} for req: {}, err: {}", 
                resp_body.body, req_body, err);
                return Errno::Eintr;
            }
        }

        if resp.result.err_code != 0 {
            println!("upload_segment: failed to upload: {}, err: {}", req_body, resp.result.err_msg);
            return Errno::Eintr;
        }

        return Errno::Esucc;
    }

    // implment heartbeat
    fn heartbeat(&self)->Result<HeartbeatResult, Errno> {
        let req = ReqHeartbeat {
            region: self.region.clone(),
            bucket: self.bucket.clone(),
            zone: self.zone.clone(),
            machine: self.machine.clone(),
        };
        let req_str: String;
        let ret = json::encode_to_str::<ReqHeartbeat>(&req);
        match ret {
            Ok(ret) => {
                req_str = ret;
            }
            Err(err) => {
                println!("heartbeat: failed to encode heart req: {:?}, err: {}", req, err);
                return Err(Errno::Eintr);
            }
        }

        let url = format!("{}/v1/machine/heartbeat", self.meta_server_url);
        let resp_text: RespText;
        let ret = self.exec.get_runtime().block_on(
            self.http_client.request(&url, req_str.as_bytes(), &HttpMethod::Get, false)
        );
        match ret {
            Ok(ret) => {
                resp_text = ret;
            }
            Err(err) => {
                println!("heartbeat: failed to send heart: {} to server, err: {}", req_str, err);
                return Err(Errno::Eintr);
            }
        }

        if resp_text.status >= 300 {
            println!("heartbeat: got status error: {} for heartbeat, resp: {}", resp_text.status, resp_text.body);
            return Err(Errno::Eintr);
        }

        let mut result = HeartbeatResult{
            upload_segments: Vec::new(),
            remove_segments: Vec::new(),
        };

        let resp: RespHeartbeat;
        let ret = json::decode_from_str::<RespHeartbeat>(&resp_text.body);
        match ret {
            Ok(ret) => {
                resp = ret;
            }
            Err(err) => {
                println!("heartbeat: got error for heartbeat: {}, resp: {}, err: {}", req_str, resp_text.body, err);
                return Err(Errno::Eintr);
            }
        }
        for u in &resp.upload_segments {
            result.upload_segments.push(HeartbeatUploadSeg{
                id0: u.seg_id0,
                id1: u.seg_id1,
                offset: u.next_offset,
            });
            // TODO remove segments.
        }

        Ok(result)
    }
}

impl MetaServiceMgrImpl {
    pub fn new(meta_cfg: &Config, exec: &Executor) -> Result<MetaServiceMgrImpl, String> {
        let http_client = Arc::new(http_client::HttpClient::new(3));
        Ok(MetaServiceMgrImpl{
            http_client: http_client,
            meta_server_url: meta_cfg.metaserver_config.meta_server.clone(),
            region: meta_cfg.s3_config.region.clone(),
            bucket: meta_cfg.s3_config.bucket.clone(),
            zone: meta_cfg.zone_config.zone.clone(),
            machine: meta_cfg.zone_config.machine.clone(),
            exec: exec.clone(),
        })
    }

    fn to_msg_block(b: &Block) -> MsgBlock {
        MsgBlock{
            offset: b.offset,
            seg_start_addr: b.seg_start_addr,
            seg_end_addr: b.seg_end_addr,
            size: b.size,
        }
    }

    fn to_msg_segment(s: &Segment) -> MsgSegment {
        let mut m = MsgSegment {
            seg_id0: s.seg_id0,
            seg_id1: s.seg_id1,
            capacity: s.capacity,
            size: s.size,
            backend_size: s.backend_size,
            leader: s.leader.clone(),
            blocks: Vec::new(),
        };
        for b in &s.blocks {
            m.blocks.push(MetaServiceMgrImpl::to_msg_block(b));
        }
        return m;
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
        let ret = self.exec.get_runtime().block_on(self.http_client.request(&url, &req_body.as_bytes(), &HttpMethod::Get, false));
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
            return Err(format!("failed to read_file_attr for ino: {}, err_code: {}, err_msg: {}",
        ino, resp_attr.result.err_code, resp_attr.result.err_msg));
        }

        return Ok(resp_attr.attr);
    }

    fn read_dir_file_attr(&self, ino: u64, name: &String) -> Result<MsgFileAttr, String>{
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
        let ret = self.exec.get_runtime().block_on(self.http_client.request(&url, &req_child_file_attr_json.as_bytes(), &HttpMethod::Get, false));
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
        let resp_attr : RespDirFileAttr;
        let ret = json::decode_from_str::<RespDirFileAttr>(&resp_text.body);
        match ret {
            Ok(attr) => {
                resp_attr = attr;
            }
            Err(error) => {
                return Err(error);
            }
        }
        if resp_attr.result.err_code != 0 {
            return Err(format!("failed to get child file attrs for ino: {}, name: {}, err: {}", 
            ino, name, resp_attr.result.err_msg));
        }
        return Ok(resp_attr.attr);
    }

    fn read_dir_files(&self, ino: u64, offset: i64) -> Result<Box<RespReadDir>, String>{
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
        let ret = self.exec.get_runtime().block_on(self.http_client.request(&url, &req_read_dir_json.as_bytes(), &HttpMethod::Get, false));
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



