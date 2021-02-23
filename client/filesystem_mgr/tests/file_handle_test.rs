use filesystem_mgr::file_handle::FileHandleMgr;
use filesystem_mgr::types::{FileHandle, Segment};

#[test]
fn test_file_handle_mgr_start()->Result<(), String> {
    let mut mgr = FileHandleMgr::create();
    mgr.stop();
    return Ok(());
}

#[test]
fn test_file_handle_mgr_add() -> Result<(), String>{
    let mut mgr = FileHandleMgr::create();
    let h1 = FileHandle{
        ino: 1,
        segments: Vec::<Segment>::new(),
    };
    let ret = mgr.add(&h1);
    if !ret.is_success(){
        mgr.stop();
        return Err(String::from("failed to add handle."));
    }

    let ret = mgr.get(1);
    match ret {
        Ok(ret) => {
            if ret.ino == h1.ino {
                mgr.stop();
                return Ok(());
            }
            mgr.stop();
            return Err(format!("got invalid handle of ino: {}", ret.ino));
        }
        Err(_) => {
            mgr.stop();
            return Err(format!("failed to get handle"));
        }
    }
}

#[test]
fn test_file_handle_mgr_del() -> Result<(), String>{
    let ino: u64 = 1;
    let mut mgr = FileHandleMgr::create();
    let h1 = FileHandle{
        ino: ino,
        segments: Vec::<Segment>::new(),
    };
    let ret = mgr.add(&h1);
    if !ret.is_success(){
        mgr.stop();
        return Err(String::from("estfailed to add handle."));
    }

    let ret = mgr.get(ino);
    match ret {
        Ok(ret) => {
            if ret.ino != h1.ino {
                mgr.stop();
                return Err(format!("got invalid handle of ino: {}", ret.ino));
            }
        }
        Err(_) => {
            mgr.stop();
            return Err(format!("failed to get handle"));
        }
    }

    let ret = mgr.del(ino);
    if !ret.is_success() {
        mgr.stop();
        return Err(String::from("failed to del file handle."));
    }

    let ret = mgr.get(ino);
    match ret {
        Ok(ret) => {
            mgr.stop();
            return Err(format!("got valid handle of ino: {} even if the handle is removed.", ret.ino));
        }
        Err(_) => {
            mgr.stop();
            return Ok(());
        }
    }
}