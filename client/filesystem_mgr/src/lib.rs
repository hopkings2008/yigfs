pub mod yigfs;
mod handle;

use std::rc::Rc;
use common::runtime::Executor;
use yigfs::Yigfs;
use metaservice_mgr::mgr::MetaServiceMgr;
use segment_mgr::segment_mgr::SegmentMgr;

pub struct MountOptions{
    // mount point
    pub mnt: String,
}

pub struct FilesystemMgr {
   meta_service_mgr: Rc<dyn MetaServiceMgr>,
   segment_mgr: Rc<SegmentMgr>,
   exec: Executor,
}

impl FilesystemMgr{
    pub fn create(meta_service_mgr: Rc<dyn MetaServiceMgr>, segment_mgr: Rc<SegmentMgr>, exec: &Executor)->FilesystemMgr{
        FilesystemMgr{
            meta_service_mgr: meta_service_mgr,
            segment_mgr: segment_mgr,
            exec: exec.clone(),
        }
    }

    pub fn mount(&self, mount_options : MountOptions) {
        let yfs = Yigfs::create(self.meta_service_mgr.clone(), self.segment_mgr.clone(), &self.exec);
        let ret = fuse::mount(yfs, &mount_options.mnt, &[]);
        match ret {
            Ok(_) => {
            }
            Err(error) => {
                println!("failed to perform mount with error: {}", error);
            }
        }
    }
}