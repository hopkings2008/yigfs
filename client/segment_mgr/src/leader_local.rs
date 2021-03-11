use tokio::sync::mpsc;
use common::runtime::Executor;
use common::error::Errno;
use io_engine::io_thread_pool::IoThreadPool;
use io_engine::types::{MsgFileOpenOp, MsgFileOp, MsgFileWriteOp, MsgFileWriteResp};
use crate::leader::Leader;
use crate::types::{SegmentIo, BlockIo};

pub struct LeaderLocal {
    io_pool: IoThreadPool,
    exec: Executor,
}

impl Leader for LeaderLocal {
    fn open(&self, seg: &SegmentIo) -> Errno {
        let worker = self.io_pool.get_worker(seg.id0, seg.id1);
        let (tx, mut rx) = mpsc::channel::<Errno>(1);
        let msg = MsgFileOpenOp{
            id0: seg.id0,
            id1: seg.id1,
            dir: seg.dir.clone(),
            resp_sender: tx,
        };
        let ret = worker.send_disk_io(MsgFileOp::OpOpen(msg));
        if !ret.is_success() {
            println!("open(id0: {}, id1: {}): failed to send open msg, err: {:?}",
            seg.id0, seg.id1, ret);
            return ret;
        }
        let ret = self.exec.get_runtime().block_on(rx.recv());
        if let Some(e) = ret {
            return e;
        }
        println!("open_segment(id0: {}, id1: {}): got invalid ret", seg.id0, seg.id1);
        return Errno::Eintr;
    }

    fn write(&self, seg: &SegmentIo, data: &[u8]) -> Result<BlockIo, Errno> {
        let worker = self.io_pool.get_worker(seg.id0, seg.id1);
        let (tx, mut rx) = mpsc::channel::<MsgFileWriteResp>(1);
        let msg = MsgFileWriteOp{
            id0: seg.id0,
            id1: seg.id1,
            offset: 0, // this element is not used currently.
            data: data.to_vec(),
            resp_sender: tx,
        };
        let ret = worker.send_disk_io(MsgFileOp::OpWrite(msg));
        if !ret.is_success() {
            println!("write: failed to send_disk_io for seg({:?}), err: {:?}",
            seg, ret);
            return Err(Errno::Eintr);
        }
        let ret = self.exec.get_runtime().block_on(rx.recv());
        if let Some(r) = ret {
            if !r.err.is_success() {
                println!("write: failed to write segment for seg({:?}), err: {:?}",
                seg, r.err);
                return Err(r.err);
            }
            // return the BlockIo
            return Ok(BlockIo{
                id0: seg.id0,
                id1: seg.id1,
                offset: r.offset,
                size: r.nwrite,
            });
        }
        println!("write: got invalid response for seg({:?}", seg);
        return Err(Errno::Eintr);
    }
}

impl LeaderLocal {
    pub fn new(thr_num: u32, exec: &Executor) -> Self {
        LeaderLocal {
            io_pool: IoThreadPool::new(thr_num, exec),
            exec: exec.clone(),
        }
    }
}