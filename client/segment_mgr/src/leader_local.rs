use std::rc::Rc;
use tokio::sync::mpsc;
use common::runtime::Executor;
use common::error::Errno;
use io_engine::io_thread_pool::IoThreadPool;
use io_engine::types::{MsgFileOpenOp, MsgFileOp, MsgFileWriteOp, MsgFileWriteResp};
use crate::leader::Leader;
use crate::file_handle::FileHandleMgr;
use crate::types::{FileHandle, Block, BlockIo, Segment};
use crate::segment_mgr::SegmentMgr;

pub struct LeaderLocal {
    machine: String,
    io_pool: IoThreadPool,
    exec: Executor,
    segment_mgr: Rc<SegmentMgr>,
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
            let worker = self.io_pool.get_worker(seg.seg_id0, seg.seg_id1);
            let (tx, mut rx) = mpsc::channel::<Errno>(1);
            let msg = MsgFileOpenOp{
                id0: seg.seg_id0,
                id1: seg.seg_id1,
                dir: seg_dir,
                resp_sender: tx,
            };
            let ret = worker.send_disk_io(MsgFileOp::OpOpen(msg));
            if !ret.is_success() {
                println!("open(id0: {}, id1: {}): failed to send open msg, err: {:?}",
                seg.seg_id0, seg.seg_id1, ret);
                return ret;
            }
            let ret = self.exec.get_runtime().block_on(rx.recv());
            if let Some(e) = ret {
                if !e.is_success() {
                    println!("open(id0: {}, id1: {}) failed with errno: {:?}", seg.seg_id0, seg.seg_id1, e);
                    return e;
                }
                continue;
            }
            println!("open(id0: {}, id1: {}): got invalid ret", seg.seg_id0, seg.seg_id1);
            return Errno::Eintr;
        }

        let file_handle = FileHandle {
            ino: ino,
            leader: self.machine.clone(),
            segments: segments,
        };
        self.handle_mgr.add(&file_handle);

        return Errno::Esucc;
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
        let mut id0 = last_segment[0];
        let mut id1 = last_segment[1];
        let mut seg_max_size = last_segment[2];
        loop {
            println!("write: seg(id0: {}, id1: {}, max_size: {})", id0, id1, seg_max_size);
            let worker = self.io_pool.get_worker(id0, id1);
            let seg_dir = self.segment_mgr.get_segment_dir(id0, id1);
            let (tx, mut rx) = mpsc::channel::<MsgFileWriteResp>(1);
            let msg = MsgFileWriteOp{
                id0: id0,
                id1: id1,
                max_size: seg_max_size,
                dir: seg_dir.clone(),
                offset: 0, // the file offset is not used currently.
                data: data.to_vec(),
                resp_sender: tx,
            };
            let ret = worker.send_disk_io(MsgFileOp::OpWrite(msg));
            if !ret.is_success() {
                println!("write: failed to send_disk_io for ino: {}, seg(id0: {}, id1: {}), err: {:?}",
                ino, id0, id1, ret);
                return Err(Errno::Eintr);
            }
            let ret = self.exec.get_runtime().block_on(rx.recv());
            if let Some(r) = ret {
                if !r.err.is_success() {
                    if r.err.is_enospc() {
                        println!("write: segment(id0: {}, id1: {}, dir: {}) has no space left for ino: {}",
                        id0, id1, seg_dir, ino);
                        let seg = self.segment_mgr.new_segment(&String::from(""));
                        self.handle_mgr.add_segment(ino, &seg);
                        id0 = seg.seg_id0;
                        id1 = seg.seg_id1;
                        seg_max_size = seg.max_size;
                        continue;
                    }
                    println!("write: failed to write segment(id0: {}, id1: {}) for ino: {}, err: {:?}",
                    id0, id1, ino, r.err);
                    return Err(r.err);
                }
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
                if ret.is_success() {
                    // return the BlockIo
                    return Ok(BlockIo{
                        id0: id0,
                        id1: id1,
                        offset: r.offset,
                        size: r.nwrite,
                    });
                }
                println!("write: failed to add block{:?} for ino: {}, err: {:?}", b, ino, ret);
                return Err(ret);
            }
            println!("write: got invalid response for seg(id0: {}, id1: {}) of ino: {}", 
            id0, id1, ino);
            return Err(Errno::Eintr);
        }
    }

    fn close(&self, ino: u64) -> Errno {
        let err = self.handle_mgr.del(ino);
        return err;
    }

    fn release(&mut self) {
        self.handle_mgr.stop();
    }
}

impl LeaderLocal {
    pub fn new(machine: &String, thr_num: u32, exec: &Executor, mgr: Rc<SegmentMgr>) -> Self {
        LeaderLocal {
            machine: machine.clone(),
            io_pool: IoThreadPool::new(thr_num, exec),
            exec: exec.clone(),
            segment_mgr: mgr,
            handle_mgr: FileHandleMgr::create(),
        }
    }
}