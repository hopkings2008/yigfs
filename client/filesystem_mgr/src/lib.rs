mod yigfs;
use yigfs::Yigfs;
use metaservice_mgr::mgr::MetaServiceMgr;

pub struct MountOptions{
    // mount point
    pub mnt: String,
}

pub struct FilesystemMgr {
   meta_service_mgr: Box<dyn MetaServiceMgr>,
}

impl FilesystemMgr{
    pub fn create(meta_service_mgr: Box<dyn MetaServiceMgr>)->FilesystemMgr{
        FilesystemMgr{
            meta_service_mgr: meta_service_mgr,
        }
    }

    pub fn mount(&self, mount_options : MountOptions) {
        let yfs = Yigfs{
            meta_service_mgr: &self.meta_service_mgr,
        };
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