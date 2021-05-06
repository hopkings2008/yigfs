pub mod yigfs;
mod handle;

use std::sync::Arc;
use yigfs::Yigfs;
use metaservice_mgr::mgr::MetaServiceMgr;
use segment_mgr::{heartbeat_mgr::HeartbeatMgr, leader_mgr::LeaderMgr};

pub struct MountOptions{
    // mount point
    pub mnt: String,
}

pub struct FilesystemMgr {
   meta_service_mgr: Arc<dyn MetaServiceMgr>,
   leader_mgr: Option<LeaderMgr>,
   heartbeat_mgr: Arc<HeartbeatMgr>,
}

impl FilesystemMgr{
    pub fn create(meta_service_mgr: Arc<dyn MetaServiceMgr>, leader_mgr: LeaderMgr, heartbeat_mgr: Arc<HeartbeatMgr>)->FilesystemMgr{
        FilesystemMgr{
            meta_service_mgr: meta_service_mgr,
            leader_mgr: Some(leader_mgr),
            heartbeat_mgr: heartbeat_mgr,
        }
    }

    pub fn mount(&mut self, mount_options : MountOptions) {
        if let Some(leader_mgr) = self.leader_mgr.take() {
            let yfs = Yigfs::create(self.meta_service_mgr.clone(), leader_mgr);
            fuse::mount(yfs, &mount_options.mnt, &[]).unwrap();
        }
    }
}