mod yigfs;
use metaservice_mgr;

pub struct MountOptions{
    // mount point
    pub mnt: String,
}

pub struct FilesystemMgr {
    metaServiceMgr: Box<dyn metaservice_mgr::mgr::MetaServiceMgr>,
}

impl FilesystemMgr{
    pub fn create(metaServiceMgr: Box<dyn metaservice_mgr::mgr::MetaServiceMgr>)->FilesystemMgr{
        FilesystemMgr{
            metaServiceMgr: metaServiceMgr,
        }
    }

    pub fn mount(&self, mountOptions : MountOptions) {
        fuse::mount(yigfs::Yigfs, &mountOptions.mnt, &[]).unwrap();
    }
}



#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
