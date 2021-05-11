use std::sync::Arc;
use common::runtime::Executor;
use common::error::Errno;
use io_engine::cache_store::CacheStore;
use io_engine::backend_storage::BackendStore;
use crate::{leader::Leader, segment_sync::SegSyncer};
use crate::file_handle::FileHandleMgr;
use crate::types::{FileHandle, Block, BlockIo, Segment};
use crate::segment_mgr::SegmentMgr;

pub struct LeaderLocal {
    machine: String,
    cache_store: Arc<dyn CacheStore>,
    backend_store: Arc<dyn BackendStore>,
    exec: Executor,
    sync_mgr: Arc<SegSyncer>,
    segment_mgr: Arc<SegmentMgr>,
    handle_mgr: FileHandleMgr,
}

impl Leader for LeaderLocal {
    fn open(&self, ino: u64) -> Errno {
        let segments : Vec<Segment>;
        let ret = self.handle_mgr.get(ino);
        match ret {
            Ok(ret) => {
                println!("open: got handle for ino: {}, leader: {}", ino, ret.leader);
                return Errno::Esucc;
            }
            Err(err) => {
                if !err.is_enoent() {
                    println!("open: failed to get file handle for ino: {}, err: {:?}", ino, err);
                    return err;
                }
            }
        }
        let ret = self.segment_mgr.get_file_segments(ino, &self.machine);
        match ret {
            Ok(ret) => {
                segments = ret;
            }
            Err(err) => {
                println!("open: failed to get_file_segments for ino: {}", ino);
                return err;
            }
        }
        for seg in &segments {
            let seg_dir = self.segment_mgr.get_segment_dir(seg.seg_id0, seg.seg_id1);
            // currently, open in cache_store will create the seg file if it doesn't exist.
            let ret = self.cache_store.open(seg.seg_id0, seg.seg_id1, &seg_dir);
            if ret.is_success(){
                // try to perform sync from backend store.
                // check whether need to perform download from backend store.
                let ret = self.cache_store.stat(seg.seg_id0, seg.seg_id1);
                match ret {
                    Ok(ret) => {
                        if ret.size < seg.size {
                            // perform the download from backend store.
                            let sync_offset = ret.size;
                            let ret = self.sync_mgr.download_segment(&seg_dir, seg.seg_id0, seg.seg_id1, sync_offset, seg.capacity);
                            if ret.is_success(){
                                println!("open: start performing downloading seg id0: {}, id1: {}, offset: {} in dir: {}",
                                seg.seg_id0, seg.seg_id1, sync_offset, seg_dir);
                            } else {
                                println!("open: failed to perform segment sync for id0: {}, id1: {}, offset: {}, dir: {}, err: {:?}",
                                seg.seg_id0, seg.seg_id1, sync_offset, seg_dir, ret);
                            }
                        }
                    }
                    Err(err) => {
                        println!("open: failed to stat seg id0: {}, id1: {}, err: {:?}", seg.seg_id0, seg.seg_id1, err);
                    }
                }
                continue;
            }
            println!("LeaderLocal open: seg(id0: {}, id1: {}) for ino: {} failed, err: {:?}",
            seg.seg_id0, seg.seg_id1, ino, ret);
            return ret;
        }

        let file_handle = FileHandle {
            ino: ino,
            leader: self.machine.clone(),
            segments: segments,
            is_dirty: 0,
        };
        self.handle_mgr.add(&file_handle);

        return Errno::Esucc;
    }

    fn read(&self, ino: u64, offset: u64, size: u32) -> Result<Vec<u8>, Errno> {
        let file_handle : FileHandle;
        let ret = self.handle_mgr.get(ino);
        match ret{
            Ok(ret) => {
                file_handle = ret;
            }
            Err(err) => {
                println!("read: failed to get file_handle for ino: {}, offset: {}, size: {}, err: {:?}", 
                ino, offset, size, err);
                return Err(err);
            }
        }
        let mut start = offset;
        let mut total_read = size;
        let mut data = Vec::<u8>::new();
        for s in &file_handle.segments {
            for b in &s.blocks {
                if b.offset <= start && start <= (b.offset + b.size as u64) {
                    let mut to_read = total_read;
                    if to_read >= b.size as u32 {
                        to_read = b.size as u32;
                    }
                    // read the data.
                    let mut need_read_backend_store = false;
                    let seg_dir = self.segment_mgr.get_segment_dir(s.seg_id0, s.seg_id1);
                    let seg_offset = b.seg_start_addr + start - b.offset;
                    let ret = self.cache_store.read(s.seg_id0, 
                        s.seg_id1, &seg_dir, seg_offset, to_read);
                    match ret {
                        Ok(ret) => {
                            if let Some(d) = ret{
                                let l = d.len() as u32;
                                total_read -= l as u32;
                                start += l as u64;
                                data.extend(d);
                            }
                        }
                        Err(err) => {
                            if err.is_eof() {
                                println!("LeadLocal: read: ino: {}, got eof for seg(id0: {}, id1: {}) offset: {}, start: {}, size: {}",
                                ino, s.seg_id0, s.seg_id1, offset, start, to_read);
                                continue;
                            }
                            if err.is_bad_offset(){
                                need_read_backend_store = true;
                            } else {
                                println!("LeadLocal: read: failed to read for ino: {}, offset: {}, start: {}, size: {}, err: {:?}", 
                                ino, offset, start, to_read, err);
                                return Err(err);
                            }
                        }
                    }
                    // need to read the backend store?
                    if need_read_backend_store {
                        let ret = self.backend_store.read(s.seg_id0, 
                            s.seg_id1, seg_offset, to_read);
                        match ret {
                            Ok(ret) => {
                                if let Some(d) = ret {
                                    let l = d.len() as u32;
                                    total_read -= l as u32;
                                    start += l as u64;
                                    data.extend(d);
                                }
                            }
                            Err(err) => {
                                if err.is_invalid_range() {
                                    println!("LeadLocal: backend_read: ino: {}, offset: {}, start: {}, size: {} 
                                    exceeds the backend store's range", ino, offset, start, to_read);
                                    continue;
                                }
                            }
                        }
                    }
                    if total_read == 0 {
                        println!("LeadLocal: read: finished for ino: {}, offset: {}, start: {}, size: {}", 
                        ino, offset, start, size);
                        return Ok(data);
                    }
                }
            }
        }
        return Ok(data);
    }

    fn write(&self, ino: u64, offset: u64, data: &[u8]) -> Result<BlockIo, Errno> {
        let last_segment: Vec<u64>;
        let ret = self.handle_mgr.get_last_segment(ino);
        match ret {
            Ok(ret) => {
                last_segment = ret;
            }
            Err(err) => {
                println!("write: failed to get_last_segment for ino: {}, err: {:?}", ino, err);
                return Err(err);
            }
        }
        if last_segment.is_empty() {
            println!("write: failed to get_last_segment for ino: {}, no segments found.", ino);
            return Err(Errno::Enoent);
        }
        let mut id0 = last_segment[0];
        let mut id1 = last_segment[1];
        let mut seg_max_size = last_segment[2];
        //println!("write: seg(id0: {}, id1: {}, max_size: {}, ino: {}, offset: {})", id0, id1, seg_max_size, ino, offset);
        loop {
            //println!("write: seg(id0: {}, id1: {}, max_size: {})", id0, id1, seg_max_size);
            let seg_dir = self.segment_mgr.get_segment_dir(id0, id1);
            let ret = self.cache_store.write(id0, id1, &seg_dir, offset, seg_max_size, data);
            match ret {
                Ok(r) => {
                    // write block success.
                    let b = Block {
                        ino: ino,
                        generation: 0,
                        offset: offset,
                        seg_start_addr: r.offset,
                        seg_end_addr: r.offset + r.nwrite as u64,
                        size: r.nwrite as i64,
                    };
                    let ret = self.handle_mgr.add_block(ino, id0, id1, &b);
                    if !ret.is_success() {
                        println!("write: failed to add_block{:?} for ino: {} with offset: {}, err: {:?}", b, ino, offset, ret);
                        return Err(ret);
                    }
                    
                    return Ok(BlockIo{
                        id0: id0,
                        id1: id1,
                        offset: r.offset,
                        size: r.nwrite,
                    });
                    // currently, will update the segments in close api.
                    // upload the block to meta server.
                    /*let ret = self.segment_mgr.upload_block(ino, id0, id1, &b);
                    if ret.is_success(){
                        return Ok(BlockIo{
                            id0: id0,
                            id1: id1,
                            offset: r.offset,
                            size: r.nwrite,
                        });
                    }
                    println!("write: failed to upload block{:?} for ino: {}, err: {:?}", b, ino, ret);
                    return Err(ret);*/
                }
                Err(err) => {
                    if err.is_enospc() {
                        println!("LeadLocal: write: segment(id0: {}, id1: {}, dir: {}) has no space left for ino: {} with offset: {}",
                            id0, id1, seg_dir, ino, offset);
                        let seg = self.segment_mgr.new_segment(&String::from(""));
                        self.handle_mgr.add_segment(ino, &seg);
                        id0 = seg.seg_id0;
                        id1 = seg.seg_id1;
                        seg_max_size = seg.capacity;
                        println!("LeadLocal: write: add new segment(id0: {}, id1: {}) for ino: {} with offset: {}",
                    id0, id1, ino, offset);
                        continue;
                    }
                    println!("LeadLocal: write: failed to get response for seg(id0: {}, id1: {}) of ino: {} with offset: {}, err: {:?}", 
                        id0, id1, ino, offset, err);
                    return Err(err);
                }
            }
            
        }
    }

    fn close(&self, ino: u64) -> Errno {
        // first we should update the segments into meta server.
        // second we should close all the file handles for the ino.
        let handle: FileHandle;
        let ret = self.handle_mgr.get(ino);
        match ret {
            Ok(ret) => {
                handle = ret;
            }
            Err(err) => {
                println!("LeadLocal close: failed to get file handle for ino: {}, err: {:?}", ino, err);
                return err;
            }
        }
        // update the segments into meta server.
        if handle.is_dirty() && !handle.segments.is_empty() {
            let ret = self.segment_mgr.update_segments(ino, &handle.segments);
            if !ret.is_success(){
                println!("LeadLocal close: failed to update segments for ino: {}, err: {:?}", ino, ret);
                return ret;
            }
        }
        // close the segments file handles.
        for s in &handle.segments {
            for b in &s.blocks {
                println!("ino: {}, seg: {}, {}, block: offset: {}, size: {}",
                ino, s.seg_id0, s.seg_id1, b.offset, b.size);
            }
            //close the segment.
            let ret = self.cache_store.close(s.seg_id0, s.seg_id1);
            if ret.is_success(){
                continue;
            }
            println!("LeadLocal: close: failed to close seg: (id0: {}, id1: {}), err: {:?}",
            s.seg_id0, s.seg_id1, ret);
        }

        let err = self.handle_mgr.del(ino);
        return err;
    }

    fn release(&mut self) {
        self.handle_mgr.stop();
    }
}

impl LeaderLocal {
    pub fn new(machine: &String, exec: &Executor, mgr: Arc<SegmentMgr>, 
        cache: Arc<dyn CacheStore>, backend: Arc<dyn BackendStore>,
        sync_mgr: Arc<SegSyncer>) -> Self {
        LeaderLocal {
            machine: machine.clone(),
            cache_store: cache,
            backend_store: backend,
            exec: exec.clone(),
            sync_mgr: sync_mgr,
            segment_mgr: mgr,
            handle_mgr: FileHandleMgr::create(),
        }
    }
}