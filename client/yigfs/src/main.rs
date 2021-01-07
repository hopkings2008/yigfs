mod options;

use filesystem_mgr;
use metaservice_mgr;

fn main() {
    let opts = options::parse();
    println!("{:?}", opts);

    let mut metaservice = metaservice_mgr::CreateMetaSerivceMgr().unwrap();
    let mut filesystem = filesystem_mgr::FilesystemMgr::create(metaservice);
    let mount_options = filesystem_mgr::MountOptions{
        mnt: opts.mnt,
    };
    filesystem.mount(mount_options);
}
