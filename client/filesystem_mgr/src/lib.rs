pub mod types;
pub mod file_handle;
pub mod yigfs;
use yigfs::Yigfs;
use metaservice_mgr::mgr::MetaServiceMgr;
use segment_mgr::segment_mgr::SegmentMgr;

pub struct MountOptions{
    // mount point
    pub mnt: String,
}

pub struct FilesystemMgr<'a> {
   meta_service_mgr: &'a Box<dyn MetaServiceMgr>,
   segment_mgr: &'a Box<SegmentMgr<'a>>,
}

impl<'a> FilesystemMgr<'a>{
    pub fn create(meta_service_mgr: &'a Box<dyn MetaServiceMgr>, segment_mgr: &'a Box<SegmentMgr<'a>>)->FilesystemMgr<'a>{
        FilesystemMgr{
            meta_service_mgr: meta_service_mgr,
            segment_mgr: segment_mgr,
        }
    }

    pub fn mount(&self, mount_options : MountOptions) {
        let yfs = Yigfs::create(&self.meta_service_mgr, &self.segment_mgr);
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