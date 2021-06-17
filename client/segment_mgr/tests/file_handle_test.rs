use std::collections::HashMap;
use segment_mgr::{file_handle::FileHandleMgr, types::Block};
use segment_mgr::types::{FileHandle, Segment};
use interval_tree::tree::IntervalTree;

#[test]
fn test_file_handle_mgr_start()->Result<(), String> {
    let mut mgr = FileHandleMgr::create();
    mgr.stop();
    let ret = mgr.get(1);
    match ret {
        Ok(h) => {
            return Err(format!("got handle with ino: {} after stop", h.ino));
        }
        Err(err) => {
            if err.is_enoent() {
                return Err(format!("got non exists error after stop"));
            }
            return Ok(());
        }
    }
}

#[test]
fn test_file_handle_mgr_add() -> Result<(), String>{
    let mut mgr = FileHandleMgr::create();
    let h1 = FileHandle{
        ino: 1,
        leader: String::from(""),
        segments: Vec::new(),
        garbage_blocks: HashMap::new(),
        block_tree: IntervalTree::new(Block::default()),
        is_dirty: 0,
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
        leader: String::from(""),
        segments: Vec::new(),
        garbage_blocks: HashMap::new(),
        block_tree: IntervalTree::new(Block::default()),
        is_dirty: 0,
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
        Err(err) => {
            mgr.stop();
            if !err.is_exists(){
                return Ok(());
            }
            return Err(format!("the get api returns the incorrect error: {:?}", err));
        }
    }
}

#[test]
fn test_file_handle_get_last_segment() -> Result<(), String>{
    let ino: u64 = 1;
    let mut mgr = FileHandleMgr::create();
    let h1 = FileHandle{
        ino: ino,
        leader: String::from(""),
        segments: Vec::new(),
        garbage_blocks: HashMap::new(),
        block_tree: IntervalTree::new(Block::default()),
        is_dirty: 0,
    };
    let ret = mgr.add(&h1);
    if !ret.is_success(){
        mgr.stop();
        return Err(String::from("estfailed to add handle."));
    }

    let ret = mgr.get_last_segment(ino);
    match ret {
        Ok(ret) => {
            if !ret.is_empty() {
                mgr.stop();
                return Err(format!("got invalid segment for ino: {}", ino));
            }
        }
        Err(_) => {
            mgr.stop();
            return Err(format!("failed to get last segments"));
        }
    }
    let seg = Segment::new(&String::from("local"));
    let id0 = seg.seg_id0;
    let id1 = seg.seg_id1;
    let ret = mgr.add_segment(ino, &seg);
    if !ret.is_success() {
        mgr.stop();
        return Err(format!("failed to add segment"));
    }
    let ret = mgr.get_last_segment(ino);
    match ret {
        Ok(ret) => {
            if ret.is_empty() {
                mgr.stop();
                return Err(format!("got empty segments"));
            }
            if ret[0] != id0 || ret[1] != id1 {
                mgr.stop();
                return Err(format!("got invalid segments, needs: id0: {}, id1: {}, but got: id0: {}, id1: {}",
            id0, id1, ret[0], ret[1]));
            }
        }
        Err(err) => {
            mgr.stop();
            return Err(format!("failed to get last segment, err: {:?}", err));
        }
    }
    let b1 = Block{
        ino: ino,
        generation: 0,
        offset: 0,
        seg_id0: id0,
        seg_id1: id1,
        seg_start_addr: 0,
        seg_end_addr: 5,
        size: 5,
    };
    let ret = mgr.add_block(ino, id0, id1, &b1);
    if !ret.is_success(){
        mgr.stop();
        return Err(format!("failed to add block"));
    }
    let seg1 = Segment::new(&String::from("local"));
    mgr.add_segment(ino, &seg1);
    let ret = mgr.get_last_segment(ino);
    match ret {
        Ok(ret) => {
            if ret[0] != seg1.seg_id0 || ret[1] != seg1.seg_id1{
                mgr.stop();
                return Err(format!("got invalid last segment, needs: id0: {}, id1: {}, but got id0: {}, id1: {}",
                seg1.seg_id0, seg1.seg_id1, ret[0], ret[1]));
            }
        }
        Err(err) => {
            mgr.stop();
            return Err(format!("failed to get last segment: err: {:?}", err));
        }
    }
    let b2 = Block{
        ino: ino,
        generation: 0,
        offset: 5,
        seg_id0: seg1.seg_id0,
        seg_id1: seg1.seg_id1,
        seg_start_addr: 5,
        seg_end_addr: 10,
        size: 5,
    };
    mgr.add_block(ino, seg1.seg_id0, seg1.seg_id1, &b2);
    let ret = mgr.get_last_segment(ino);
    match ret {
        Ok(ret) => {
            if ret[0] != seg1.seg_id0 || ret[1] != seg1.seg_id1{
                mgr.stop();
                return Err(format!("test final: got invalid last segment, needs: id0: {}, id1: {}, but got id0: {}, id1: {}",
                seg1.seg_id0, seg1.seg_id1, ret[0], ret[1]));
            }
        }
        Err(err) => {
            mgr.stop();
            return Err(format!("test final: failed to get last segment: err: {:?}", err));
        }
    }
    mgr.stop();
    return Ok(());
}