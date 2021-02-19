mod types;
use types::FileHandle;
use std::collections::HashMap;

pub struct FileHandleMgr {
    // ino-->FileHandle
    handle_map: HashMap<i64, FileHandle>,
}