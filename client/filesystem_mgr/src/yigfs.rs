extern crate fuse;
extern crate libc;
extern crate time;

use std::ffi::OsStr;
use libc::{c_int, ENOENT};
use time::Timespec;
use fuse::{FileType, FileAttr, Filesystem, Request, 
    ReplyData, ReplyEntry, ReplyAttr, ReplyDirectory, ReplyCreate};
use metaservice_mgr::{mgr::MetaServiceMgr, types::{FileLeader, NewFileInfo, SetFileAttr}};

const TTL: Timespec = Timespec { sec: 1, nsec: 0 };                     // 1 second

/*const HELLO_DIR_ATTR: FileAttr = FileAttr {
    ino: 1,
    size: 0,
    blocks: 0,
    atime: CREATE_TIME,
    mtime: CREATE_TIME,
    ctime: CREATE_TIME,
    crtime: CREATE_TIME,
    kind: FileType::Directory,
    perm: 0o755,
    nlink: 2,
    uid: 501,
    gid: 20,
    rdev: 0,
    flags: 0,
};*/

const HELLO_TXT_CONTENT: &'static str = "Hello World!\n";

/*const HELLO_TXT_ATTR: FileAttr = FileAttr {
    ino: 2,
    size: 13,
    blocks: 1,
    atime: CREATE_TIME,
    mtime: CREATE_TIME,
    ctime: CREATE_TIME,
    crtime: CREATE_TIME,
    kind: FileType::RegularFile,
    perm: 0o644,
    nlink: 1,
    uid: 501,
    gid: 20,
    rdev: 0,
    flags: 0,
};*/

pub struct Yigfs<'a>{
    pub meta_service_mgr: &'a Box<dyn MetaServiceMgr>,
}

impl<'a> Filesystem for Yigfs<'a> {
    fn init(&mut self, req: &Request) -> Result<(), c_int> {
        let ret = self.meta_service_mgr.mount(req.uid(), req.gid());
        match ret {
            Ok(_) => {
                return Ok(());
            }
            Err(error) => {
                println!("failed to mount with err: {:?}", error);
                return Err(ENOENT);
            }
        }
    }
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str: String;
        let ret = name.to_str();
        match ret {
            Some(ret) => {
                name_str = String::from(ret);
            }
            None => {
                println!("got invalid parent: {}, name: {:?}", parent, name);
                return;
            }
        }
        println!("lookup: parent: {}, name: {}", parent, name_str);
        let ret = self.meta_service_mgr.read_dir_file_attr(parent, &name_str);
        match ret {
            Ok(ret) => {
                let file_attr = self.to_usefs_attr(&ret);
                reply.entry(&TTL, &file_attr, ret.generation);
            }
            Err(error) => {
                println!("failed to lookup for parent: {}, name: {}, err: {:?}", parent, name_str, error);
                reply.error(ENOENT);
            }
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        let ret = self.meta_service_mgr.read_file_attr(ino);
        match ret {
            Ok(ret) => {
                let attr = self.to_usefs_attr(&ret);
                reply.attr(&TTL, &attr);
            }
            Err(error) => {
                println!("failed to getattr for ino: {}, err: {:?}", ino, error);
                reply.error(ENOENT);
            }
        }
    }

    fn setattr(&mut self, req: &Request, ino: u64, mode: Option<u32>, uid: Option<u32>, gid: Option<u32>, size: Option<u64>, atime: Option<Timespec>, mtime: Option<Timespec>, fh: Option<u64>, _crtime: Option<Timespec>, _chgtime: Option<Timespec>, _bkuptime: Option<Timespec>, _flags: Option<u32>, reply: ReplyAttr){
        println!("setattr: uid: {}, gid: {}, pid: {}", req.uid(), req.gid(), req.pid());
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

        let file_attr : metaservice_mgr::types::FileAttr;
        let ret = self.meta_service_mgr.set_file_attr(&set_attr);
        match ret {
            Ok(ret) => {
                file_attr = ret;
            }
            Err(err) => {
                println!("failed to set_file_attr for {:?}, err: {:?}", set_attr, err);
                reply.error(libc::EBADEXEC);
                return;
            }
        }
        reply.attr(&TTL, &self.to_usefs_attr(&file_attr));
    }

    fn read(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, _size: u32, reply: ReplyData) {
        if ino == 2 {
            reply.data(&HELLO_TXT_CONTENT.as_bytes()[offset as usize..]);
        } else {
            reply.error(ENOENT);
        }
    }

    fn readdir(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
        // must authorize the request here by checking _req.
        println!("readdir: ino: {}, offset: {}", ino, offset);
        let entrys : Vec<metaservice_mgr::types::DirEntry>;
        let ret = self.meta_service_mgr.read_dir(ino, offset);
        match ret {
            Ok(ret) => {
                entrys = ret;
            }
            Err(error) => {
                println!("failed to readdir for ino: {}, offset: {}, err: {:?}", ino, offset, error);
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
                println!("create: got invalid name: {:?}", name);
                reply.error(libc::EBADMSG);
                return;
            }
        }

        let name = string;

        println!("create: uid: {}, gid: {}, parent: {}, name: {}, mod: {}, flags: {}",
        req.uid(), req.gid(), parent, name, mode, flags);
        let file_info: NewFileInfo;
        let ret = self.meta_service_mgr.new_ino_leader(parent, &name, req.uid(), req.gid(), mode);
        match ret {
            Ok(ret ) => {
                file_info = ret;
            }
            Err(err) => {
                println!("failed to new_ino_leader: parent: {}, name: {}, err: {:?}",
                parent, name, err);
                reply.error(libc::EIO);
                return;
            }
        }
        reply.created(&TTL, &self.to_usefs_attr(&file_info.attr), file_info.attr.generation, file_info.attr.ino, flags);
    }
}

impl<'a> Yigfs<'a>{
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