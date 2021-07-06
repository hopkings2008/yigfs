
use metaservice_mgr::types::{Segment, Block};
use segment_mgr::file_handle::FileHandleMgr;
use segment_mgr::types::FileHandle;

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
    let h1 = FileHandle::new(1);
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
    let h1 = FileHandle::new(ino);
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
    let h1 = FileHandle::new(ino);
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

#[test]
fn test_file_handle_add_block() -> Result<(), String>{
    let ino: u64 = 1;
    let mut mgr = FileHandleMgr::create();
    let h1 = FileHandle::new(ino);
    let ret = mgr.add(&h1);
    if !ret.is_success(){
        mgr.stop();
        return Err(String::from("estfailed to add handle."));
    }

    let seg = Segment::new(&String::from("local"));
    let id0 = seg.seg_id0;
    let id1 = seg.seg_id1;
    let ret = mgr.add_segment(ino, &seg);
    if !ret.is_success() {
        mgr.stop();
        return Err(format!("failed to add segment"));
    }
    
    let b1 = Block{
        ino: ino,
        generation: 0,
        offset: 0,
        seg_id0: id0,
        seg_id1: id1,
        seg_start_addr: 0,
        size: 5,
    };
    let ret = mgr.add_block(ino, id0, id1, &b1);
    if !ret.is_success(){
        mgr.stop();
        return Err(format!("failed to add block"));
    }

    let blocks = mgr.get_blocks(ino, 0, 5);
    if blocks.is_empty() {
        mgr.stop();
        return Err(format!("got empty blocks for ino: {}, offset: 0, size: 5", ino));
    }
    if blocks.len() > 1 {
        for b in blocks {
            println!("got extra block: {:?}", b);
        }
        mgr.stop();
        return Err(format!("got more than 1 blocks"));
    }
    println!("got original block: {:?}", blocks[0]);
    if blocks[0].seg_id0 != seg.seg_id0 || blocks[0].seg_id1 != seg.seg_id1
    || blocks[0].offset != b1.offset || blocks[0].size != b1.size {
        mgr.stop();
        return Err(format!("got invalid block: {:?}, expect: {:?}", blocks[0], b1));
    }
    
    let b2 = Block{
        ino: ino,
        generation: 0,
        offset: 0,
        seg_id0: seg.seg_id0,
        seg_id1: seg.seg_id1,
        seg_start_addr: 5,
        size: 5,
    };
    mgr.add_block(ino, seg.seg_id0, seg.seg_id1, &b2);
    let blocks = mgr.get_blocks(ino, 0, 5);
    if blocks.is_empty() {
        mgr.stop();
        return Err(format!("got empty blocks for ino: {}, overwriten offset: 0, size: 5", ino));
    }
    if blocks.len() > 1 {
        for b in blocks {
            println!("got extra overwriten block: {:?}", b);
        }
        mgr.stop();
        return Err(format!("got more than 1 overwriten blocks"));
    }
    if blocks[0].seg_id0 != seg.seg_id0 || blocks[0].seg_id1 != seg.seg_id1
    || blocks[0].offset != b2.offset || blocks[0].size != b2.size 
    || blocks[0].seg_start_addr != 5 {
        mgr.stop();
        return Err(format!("got invalid overwriten block: {:?}, expect: {:?}", blocks[0], b1));
    }
    mgr.stop();
    return Ok(());
}