use std::sync::Arc;
use std::time::Instant;
use common::runtime::Executor;
use common::error::Errno;
use io_engine::cache_store::CacheStore;
use io_engine::backend_storage::BackendStore;
use log::{info, warn, error};
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
        let ret = self.handle_mgr.get_and_lock(ino);
        match ret {
            Ok(ret) => {
                info!("open: got handle for ino: {}, leader: {}", ino, ret.leader);
                return Errno::Esucc;
            }
            Err(err) => {
                if !err.is_enoent() {
                    error!("open: failed to get file handle for ino: {}, err: {:?}", ino, err);
                    return err;
                }
            }
        }
        let begin = Instant::now();
        let ret = self.segment_mgr.get_file_segments(ino, &self.machine);
        match ret {
            Ok(ret) => {
                segments = ret;
            }
            Err(err) => {
                error!("open: failed to get_file_segments for ino: {}", ino);
                return err;
            }
        }
        let dur = begin.elapsed().as_nanos();
        info!("get_file_segments for ino: {} takes: {}", ino, dur);
        let begin = Instant::now();
        for seg in &segments {
            let seg_dir = self.segment_mgr.get_segment_dir(seg.seg_id0, seg.seg_id1);
            // currently, open in cache_store will create the seg file if it doesn't exist.
            let ret = self.cache_store.open(seg.seg_id0, seg.seg_id1, &seg_dir);
            if ret.is_success(){
                // try to perform sync from backend store.
                // check whether need to perform download from backend store.
                let ret = self.cache_store.stat(seg.seg_id0, seg.seg_id1, &seg_dir);
                match ret {
                    Ok(ret) => {
                        if ret.size < seg.size {
                            // perform the download from backend store.
                            info!("open: seg: id0: {}, id1: {}, cache size: {}, real size: {}",
                            seg.seg_id0, seg.seg_id1, ret.size, seg.size);
                            let sync_offset = ret.size;
                            let ret = self.sync_mgr.download_segment(&seg_dir, seg.seg_id0, seg.seg_id1, sync_offset, seg.capacity);
                            if ret.is_success(){
                                info!("open: start performing downloading seg id0: {}, id1: {}, offset: {} in dir: {}",
                                seg.seg_id0, seg.seg_id1, sync_offset, seg_dir);
                            } else {
                                error!("open: failed to perform segment sync for id0: {}, id1: {}, offset: {}, dir: {}, err: {:?}",
                                seg.seg_id0, seg.seg_id1, sync_offset, seg_dir, ret);
                            }
                        }
                    }
                    Err(err) => {
                        error!("open: failed to stat seg id0: {}, id1: {}, err: {:?}", seg.seg_id0, seg.seg_id1, err);
                    }
                }
                continue;
            }
            error!("LeaderLocal open: seg(id0: {}, id1: {}) for ino: {} failed, err: {:?}",
            seg.seg_id0, seg.seg_id1, ino, ret);
            return ret;
        }

        let dur = begin.elapsed().as_nanos();
        info!("open: open segments for ino: {} takes: {}", ino, dur);
        let begin = Instant::now();
        let file_handle = FileHandle::create(ino, self.machine.clone(), segments);
        self.handle_mgr.add(&file_handle);
        let dur = begin.elapsed().as_nanos();
        info!("open: add file_handle for ino: {} takes: {}", ino, dur);

        return Errno::Esucc;
    }

    fn read(&self, ino: u64, offset: u64, size: u32) -> Result<Vec<u8>, Errno> {
        let mut start = offset;
        let mut total_read = size;
        let mut data = Vec::<u8>::new();
        let blocks = self.handle_mgr.get_blocks(ino, offset, size as u64);
        for b in &blocks {
            if b.offset <= start && start <= (b.offset + b.size as u64) {
                let mut to_read = total_read;
                if to_read >= b.size as u32 {
                    to_read = b.size as u32;
                }
                // read the data.
                let mut need_read_backend_store = false;
                let seg_dir = self.segment_mgr.get_segment_dir(b.seg_id0, b.seg_id1);
                let seg_offset = b.seg_start_addr + start - b.offset;
                let ret = self.cache_store.read(b.seg_id0, 
                    b.seg_id1, &seg_dir, seg_offset, to_read);
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
                            info!("LeadLocal: read: ino: {}, got eof for seg(id0: {}, id1: {}) offset: {}, start: {}, size: {}",
                            ino, b.seg_id0, b.seg_id1, offset, start, to_read);
                            continue;
                        }
                        if err.is_bad_offset(){
                            need_read_backend_store = true;
                        } else {
                            error!("LeadLocal: read: failed to read for ino: {}, offset: {}, start: {}, size: {}, err: {:?}", 
                            ino, offset, start, to_read, err);
                            return Err(err);
                        }
                    }
                }
                // need to read the backend store?
                if need_read_backend_store {
                    let ret = self.backend_store.read(b.seg_id0, 
                        b.seg_id1, seg_offset, to_read);
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
                                error!("LeadLocal: backend_read: ino: {}, offset: {}, start: {}, size: {} 
                                exceeds the backend store's range", ino, offset, start, to_read);
                                continue;
                            }
                        }
                    }
                }
                if total_read == 0 {
                    warn!("LeadLocal: read: finished for ino: {}, offset: {}, start: {}, size: {}", 
                    ino, offset, start, size);
                    return Ok(data);
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
                error!("write: failed to get_last_segment for ino: {}, err: {:?}", ino, err);
                return Err(err);
            }
        }
        if last_segment.is_empty() {
            error!("write: failed to get_last_segment for ino: {}, no segments found.", ino);
            return Err(Errno::Enoent);
        }
        let mut id0 = last_segment[0];
        let mut id1 = last_segment[1];
        let mut seg_max_size = last_segment[2];
        let mut seg_size = last_segment[3];
        
        //println!("write: seg(id0: {}, id1: {}, max_size: {}, ino: {}, offset: {})", id0, id1, seg_max_size, ino, offset);
        loop {
            //println!("write: seg(id0: {}, id1: {}, max_size: {})", id0, id1, seg_max_size);
            let seg_dir = self.segment_mgr.get_segment_dir(id0, id1);
            // must check whether cache size is smaller than segment size or not. if so, write the backend directly.
            // or if O_DIRECT, write to backend directly too.
            let cache_size: u64;
            let ret = self.cache_store.stat(id0, id1, &seg_dir);
            match ret {
                Ok(ret) => {
                    cache_size = ret.size;
                }
                Err(err) => {
                    error!("write: failed to perform cache stat for seg: id0: {}, id1: {}, dir: {}, err: {:?}",
                    id0, id1, seg_dir, err);
                    cache_size = 0;
                }
            }
            if cache_size < seg_size {
                // write to backend store directly.
                let ret = self.backend_store.write(id0, id1, seg_size, data);
                if ret.err.is_success() {
                    return Ok(BlockIo{
                        id0: id0,
                        id1: id1,
                        offset: ret.offset,
                        size: ret.nwrite,
                    });
                }
                error!("write: backend_store write failed for seg: id0: {}, id1: {}, offset: {}, err: {:?}",
                id0, id1, seg_size, ret.err);
                return Err(ret.err);
            }
            let ret = self.cache_store.write(id0, id1, &seg_dir, offset, seg_max_size, data);
            match ret {
                Ok(r) => {
                    // write block success.
                    let b = Block {
                        ino: ino,
                        generation: 0,
                        offset: offset,
                        seg_id0: id0,
                        seg_id1: id1,
                        seg_start_addr: r.offset,
                        size: r.nwrite as i64,
                    };
                    let ret = self.handle_mgr.add_block(ino, id0, id1, &b);
                    if !ret.is_success() {
                        error!("write: failed to add_block{:?} for ino: {} with offset: {}, err: {:?}", b, ino, offset, ret);
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
                    error!("write: failed to upload block{:?} for ino: {}, err: {:?}", b, ino, ret);
                    return Err(ret);*/
                }
                Err(err) => {
                    if err.is_enospc() {
                        error!("LeadLocal: write: segment(id0: {}, id1: {}, dir: {}) has no space left for ino: {} with offset: {}",
                            id0, id1, seg_dir, ino, offset);
                        let seg = self.segment_mgr.new_segment(&String::from(""));
                        self.handle_mgr.add_segment(ino, &seg);
                        id0 = seg.seg_id0;
                        id1 = seg.seg_id1;
                        seg_max_size = seg.capacity;
                        // when create new segment, set the current seg_size to 0.
                        seg_size = 0;
                        info!("LeadLocal: write: add new segment(id0: {}, id1: {}) for ino: {} with offset: {}",
                    id0, id1, ino, offset);
                        continue;
                    }
                    error!("LeadLocal: write: failed to get response for seg(id0: {}, id1: {}) of ino: {} with offset: {}, err: {:?}", 
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
                error!("LeadLocal close: failed to get file handle for ino: {}, err: {:?}", ino, err);
                return err;
            }
        }
        // update the segments into meta server.
        let segments = handle.get_segments();
        if handle.is_dirty() && !segments.is_empty() {
            let ret = self.segment_mgr.update_segments(ino, &segments);
            if !ret.is_success(){
                error!("LeadLocal close: failed to update segments for ino: {}, err: {:?}", ino, ret);
                return ret;
            }
        }
        // close the segments file handles.
        for s in &segments {
            /*for b in &s.blocks {
                info!("ino: {}, seg: {}, {}, block: offset: {}, size: {}",
                ino, s.seg_id0, s.seg_id1, b.offset, b.size);
            }*/
            //close the segment.
            let ret = self.cache_store.close(s.seg_id0, s.seg_id1);
            if ret.is_success(){
                continue;
            }
            error!("LeadLocal: close: failed to close seg: (id0: {}, id1: {}), err: {:?}",
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
