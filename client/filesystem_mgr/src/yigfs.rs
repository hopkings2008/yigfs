extern crate fuse;
extern crate libc;
extern crate time;

use std::ffi::OsStr;
use std::sync::Arc;
use libc::{ENOENT, c_int};
use time::Timespec;
use fuse::{FileType, FileAttr, Filesystem, Request, 
    ReplyData, ReplyEntry, ReplyAttr, ReplyDirectory, ReplyCreate, ReplyOpen, ReplyWrite, ReplyEmpty};
use metaservice_mgr::{mgr::MetaServiceMgr, types::{FileLeader, NewFileInfo, SetFileAttr}};
use segment_mgr::leader_mgr::LeaderMgr;
use common::uuid;
use crate::handle::{FileHandleInfo, FileHandleInfoMgr};
use log::{info, warn, error};

const TTL: Timespec = Timespec { sec: 1, nsec: 0 };                     // 1 second


pub struct Yigfs{
    meta_service_mgr: Arc<dyn MetaServiceMgr>,
    leader_mgr: LeaderMgr,
    handle_cacher: FileHandleInfoMgr,
    // fsid for this mounted yigfs instance
    fsid: String,
}

impl Filesystem for Yigfs {
    fn init(&mut self, req: &Request) -> Result<(), c_int> {
        info!("init: uid: {}, gid: {}, fsid: {}", req.uid(), req.gid(), self.fsid);
        let ret = self.meta_service_mgr.mount(req.uid(), req.gid());
        match ret {
            Ok(_) => {
                return Ok(());
            }
            Err(error) => {
                error!("failed to mount with err: {:?}", error);
                return Err(ENOENT);
            }
        }
    }
    fn destroy(&mut self, req: &Request) {
        warn!("destroy: uid: {}, gid: {}, fsid: {}", req.uid(), req.gid(), self.fsid);
        self.leader_mgr.stop();
        self.handle_cacher.stop();
    }
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str: String;
        let ret = name.to_str();
        match ret {
            Some(ret) => {
                name_str = String::from(ret);
            }
            None => {
                error!("got invalid parent: {}, name: {:?}", parent, name);
                return;
            }
        }
        info!("lookup: parent: {}, name: {}", parent, name_str);
        let ret = self.meta_service_mgr.read_dir_file_attr(parent, &name_str);
        match ret {
            Ok(ret) => {
                let file_attr = self.to_usefs_attr(&ret);
                info!("lookup: parent: {}, name: {}, attr: {:?}", parent, name_str, file_attr);
                reply.entry(&TTL, &file_attr, ret.generation);
            }
            Err(error) => {
                error!("failed to lookup for parent: {}, name: {}, err: {:?}", parent, name_str, error);
                reply.error(ENOENT);
            }
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        let ret = self.meta_service_mgr.read_file_attr(ino);
        match ret {
            Ok(ret) => {
                let attr = self.to_usefs_attr(&ret);
                info!("getattr: ino: {}, attr: {:?}", ino, attr);
                reply.attr(&TTL, &attr);
            }
            Err(error) => {
                error!("failed to getattr for ino: {}, err: {:?}", ino, error);
                reply.error(ENOENT);
            }
        }
    }

    fn setattr(&mut self, req: &Request, ino: u64, mode: Option<u32>, uid: Option<u32>, gid: Option<u32>, size: Option<u64>, atime: Option<Timespec>, mtime: Option<Timespec>, _fh: Option<u64>, _crtime: Option<Timespec>, _chgtime: Option<Timespec>, _bkuptime: Option<Timespec>, _flags: Option<u32>, reply: ReplyAttr){
        let mut set_attr = SetFileAttr{
            ino: ino,
            size: size,
            atime: None,
            mtime: None,
            ctime: None,
            perm: None,
            uid: uid,
            gid: gid,
        };
        
        match mode {
            Some(m) => {
                set_attr.perm = Some(m as u16);
            }
            None => {
                set_attr.perm = None;
            }
        }
        match atime {
            Some(t) => {
                set_attr.atime = Some(common::time::ts_to_nsecs(&t));
            }
            None => {
                set_attr.atime  = None;
            }
        }
        match mtime {
            Some(t) => {
                set_attr.mtime = Some(common::time::ts_to_nsecs(&t));
            }
            None => {
                set_attr.mtime = None;
            }
        }
        set_attr.size = size;

        info!("setattr: uid: {}, gid: {}, pid: {}, attr: {:?}", req.uid(), req.gid(), req.pid(), set_attr);
        let file_attr : metaservice_mgr::types::FileAttr;
        let ret = self.meta_service_mgr.set_file_attr(&set_attr);
        match ret {
            Ok(ret) => {
                file_attr = ret;
                info!("set_attr: got result: {:?} for attr: {:?}", file_attr, set_attr);
            }
            Err(err) => {
                error!("failed to set_file_attr for {:?}, err: {:?}", set_attr, err);
                reply.error(libc::EIO);
                return;
            }
        }
        reply.attr(&TTL, &self.to_usefs_attr(&file_attr));
    }

    fn read(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, size: u32, reply: ReplyData) {
        let leader: String;
        let ret = self.handle_cacher.get_handle_info(ino);
        match ret {
            Ok(ret) => {
                leader = ret.leader;
            }
            Err(err) => {
                error!("read: file ino: {} is not opened yet, err: {:?}.", ino, err);
                reply.error(libc::EBADF);
                return;
            }
        }
        // get the leader.
        let leader_io = self.leader_mgr.get_leader(&leader);
        let ret = leader_io.read(ino, offset as u64, size);
        match ret {
            Ok(ret) => {
                reply.data(ret.as_slice());
                return;
            }
            Err(err) => {
                error!("read: failed to read ino: {}, offset: {}, err: {:?}", ino, offset, err);
                reply.error(libc::EIO);
                return;
            }
        }
    }

    fn readdir(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
        // must authorize the request here by checking _req.
        info!("readdir: ino: {}, offset: {}", ino, offset);
        let entrys : Vec<metaservice_mgr::types::DirEntry>;
        let ret = self.meta_service_mgr.read_dir(ino, offset);
        match ret {
            Ok(ret) => {
                entrys = ret;
            }
            Err(error) => {
                error!("failed to readdir for ino: {}, offset: {}, err: {:?}", ino, offset, error);
                reply.error(ENOENT);
                return;
            }
        }
        // chech whether entrys is empty.
        if entrys.is_empty(){
            reply.error(ENOENT);
            return;
        }
        
        for entry in entrys {
            reply.add(entry.ino, entry.ino as i64, self.ft_to_fuse_ft(&entry.file_type), entry.name);
        }
        reply.ok();
    }

    fn create(&mut self, req: &Request, parent: u64, name: &OsStr, mode: u32, flags: u32, reply: ReplyCreate){
        let str = name.to_str();
        let string : String;
        match str {
            Some(str) => {
                string = String::from(str);
            }
            None => {
                error!("create: got invalid name: {:?}", name);
                reply.error(libc::EBADMSG);
                return;
            }
        }

        let name = string;

        info!("create: uid: {}, gid: {}, parent: {}, name: {}, mod: {}, flags: {}",
        req.uid(), req.gid(), parent, name, mode, flags);
        let file_info: NewFileInfo;
        let ret = self.meta_service_mgr.new_ino_leader(parent, &name, req.uid(), req.gid(), mode);
        match ret {
            Ok(ret ) => {
                file_info = ret;
            }
            Err(err) => {
                if !err.is_exists() {
                    error!("failed to new_ino_leader: parent: {}, name: {}, err: {:?}",
                    parent, name, err);
                    reply.error(libc::EIO);
                    return;
                }
                warn!("new_ino_leader: parent: {}, name: {} already exists", parent, name);
                reply.error(libc::EEXIST);
                return;
            }
        }
        let ret = self.handle_cacher.add_handle_info(FileHandleInfo{
            ino: file_info.attr.ino,
            leader: file_info.leader_info.leader.clone(),
        });
        if !ret.is_success(){
            error!("create: failed to add handle cache for name: {}, ino: {}", name, file_info.attr.ino);
            reply.error(libc::EIO);
            return;
        }
        let leader_io = self.leader_mgr.get_leader(&file_info.leader_info.leader);
        let ret = leader_io.open(file_info.attr.ino);
        if !ret.is_success(){
            error!("create: failed to open name: {}, ino: {}", name, file_info.attr.ino);
            reply.error(libc::EIO);
            return;
        }
        // will check flags and set this later.
        // cache ino->leader to reduce the net io.
        reply.created(&TTL, &self.to_usefs_attr(&file_info.attr), file_info.attr.generation, file_info.attr.ino, flags);
    }

    fn open(&mut self, req: &Request, ino: u64, flags: u32, reply: ReplyOpen){
        let file_leader_info : FileLeader;
        info!("open: uid: {}, gid: {}, ino: {}, flags: {}",
        req.uid(), req.gid(), ino, flags);
        let ret = self.meta_service_mgr.get_file_leader(ino);
        match ret {
            Ok(ret) => {
                file_leader_info = ret;
                info!("got file leader {:?} for ino: {}", file_leader_info.leader, ino);
            }
            Err(err) => {
                error!("failed to get_file_leader for ino: {}, err: {:?}", ino, err);
                reply.error(libc::EBADF);
                return;
            }
        }
        let leader = self.leader_mgr.get_leader(&file_leader_info.leader);
        let ret = self.handle_cacher.add_handle_info(FileHandleInfo{
            ino: ino,
            leader: file_leader_info.leader.clone(),
        });
        if !ret.is_success() {
            error!("open: failed to add handle cache for ino: {}, leader: {}", ino, file_leader_info.leader);
            reply.error(libc::EBADF);
            return;
        }
        let ret = leader.open(ino);
        if ret.is_success() {
            reply.opened(ino, flags);
            return;
        }
        error!("open: failed to open ino: {}, err: {:?}", ino, ret);
        reply.error(libc::EIO);
        return;        
    }

    fn write(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, data: &[u8], _flags: u32, reply: ReplyWrite){
        //println!("write: uid: {}, gid: {}, ino: {}, fh: {}, offset: {}, data_size: {}, flags: {}",
        //req.uid(), req.gid(), ino, fh, offset, data.len(), flags);
        // get the file leader ip.
        let leader: String;
        let ret = self.handle_cacher.get_handle_info(ino);
        match ret {
            Ok(ret) => {
                leader = ret.leader;
            }
            Err(err) => {
                error!("write: file ino: {} is not opened yet, err: {:?}.", ino, err);
                reply.error(libc::EBADF);
                return;
            }
        }
        // get the leader.
        let leader_io = self.leader_mgr.get_leader(&leader);
        let ret = leader_io.write(ino, offset as u64, data);
        match ret {
            Ok(ret) => {
                reply.written(ret.size);
                return;
            }
            Err(err) => {
                error!("write: failed to write ino: {}, offset: {}, err: {:?}",
                ino, offset, err);
                reply.error(libc::EIO);
                return;
            }
        }
    }

    fn release(&mut self, req: &Request, ino: u64, fh: u64, flags: u32, lock_owner: u64, flush: bool, reply: ReplyEmpty) {
        info!("release: uid: {}, gid: {}, ino: {}, fh: {}, flags: {}, lock_owner: {}, flush: {}", 
        req.uid(), req.gid(), ino, fh, flags, lock_owner, flush);
        let ret = self.handle_cacher.get_handle_info(ino);
        match ret {
            Ok(ret) => {
                let leader = self.leader_mgr.get_leader(&ret.leader);
                let err = leader.close(ino);
                if !err.is_success(){
                    error!("release: failed to close ino: {}, err: {:?}", ino, err);
                }
            }
            Err(err) => {
                error!("release: failed to get handle for ino: {}, err: {:?}", ino, err);
            }
        }
        let err = self.handle_cacher.del_handle_info(ino);
        if !err.is_success() {
            error!("release: failed to del handle for ino: {}, err: {:?}", ino, err);
        }
        reply.ok();
    }

    fn unlink(&mut self, req: &Request, ino: u64, name: &OsStr, reply: ReplyEmpty) {
        let file_name: String;
        if let Some(n) = name.to_str() {
            file_name = n.to_string();
        } else {
            error!("unlink: uid: {}, gid: {}, parent ino: {}, got invalid name",
            req.uid(), req.gid(), ino);
            reply.error(libc::EBADMSG);
            return;
        }
        let file_ino: u64;
        let ret = self.meta_service_mgr.read_dir_file_attr(ino, &file_name);
        match ret {
            Ok(attr) => {
                file_ino = attr.ino;
            }
            Err(err) => {
                error!("unlink: uid: {}, gid: {}, parent ino: {}, name: {}, failed to get file attr, err: {:?}",
                req.uid(), req.gid(), ino, file_name, err);
                reply.error(libc::EBADMSG);
                return;
            }
        }
        info!("unlink: uid: {}, gid: {}, parent ino: {}, name: {}, ino: {}", 
        req.uid(), req.gid(), ino, file_name, file_ino);
        let ret = self.meta_service_mgr.delete_file(file_ino);
        if !ret.is_success(){
            error!("unlink: failed to remove the file, ino: {}", ino);
            reply.error(libc::EIO);
            return;
        }
        reply.ok();
    }
}

impl Yigfs{
    pub fn create(meta: Arc<dyn MetaServiceMgr>, leader_mgr: LeaderMgr)-> Yigfs{
        Yigfs{
            meta_service_mgr: meta,
            leader_mgr: leader_mgr,
            handle_cacher: FileHandleInfoMgr::new(),
            fsid: uuid::uuid_string(),
        }
    }
    fn to_usefs_attr(&self, attr: &metaservice_mgr::types::FileAttr) -> FileAttr {
        FileAttr{
            ino: attr.ino,
            size: attr.size,
            blocks: attr.blocks,
            atime: common::time::nsecs_to_ts(attr.atime),
            mtime: common::time::nsecs_to_ts(attr.mtime),
            ctime: common::time::nsecs_to_ts(attr.ctime),
            crtime: common::time::nsecs_to_ts(attr.ctime),
            kind: self.ft_to_fuse_ft(&attr.kind),
            perm: attr.perm,
            nlink: attr.nlink,
            uid: attr.uid,
            gid: attr.gid,
            rdev: attr.rdev,
            flags: attr.flags,
        }
    }
    
    fn ft_to_fuse_ft(&self, ft: &metaservice_mgr::types::FileType) ->FileType {
        match ft{
            metaservice_mgr::types::FileType::DIR => {
                FileType::Directory
            }
            metaservice_mgr::types::FileType::LINK => {
                FileType::Symlink
            }
            _ => {
                FileType::RegularFile
            }
        }
    }
}