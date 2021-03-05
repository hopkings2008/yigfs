extern crate tokio;

use tokio::sync::mpsc;
use crate::types::{Segment, Block};
use common::error::Errno;
use common::runtime::Executor;
use metaservice_mgr::mgr::MetaServiceMgr;
use io_engine::io_thread_pool::IoThreadPool;
use io_engine::types::{MsgFileOpenOp, MsgFileOp, MsgFileWriteOp, MsgFileWriteResp};
pub struct SegmentMgr<'a> {
    meta_service_mgr: &'a Box<dyn MetaServiceMgr>,
    data_dirs: Vec<String>,
    io_pool: IoThreadPool,
    exec: Executor,
}

impl<'a> SegmentMgr<'a> {
    pub fn get_file_segments(&self, ino: u64, leader: &String)-> Result<Vec<Segment>, Errno> {
        let mut segments : Vec<Segment> = Vec::new();
        let segs : Vec<metaservice_mgr::types::Segment>;
        let ret = self.meta_service_mgr.get_file_segments(ino, None, None);
        match ret {
            Ok(ret) => {
                segs = ret;
            }
            Err(err) => {
                println!("failed to get_file_segments for ino {}, err: {:?}", ino, err);
                return Err(err);
            }
        }
        if segs.is_empty() {
            let seg = Segment::new(leader);
            segments.push(seg);
            return Ok(segments);
        }
        for s in segs {
            let mut segment : Segment = Default::default();
            segment.seg_id0 = s.seg_id0;
            segment.seg_id1 = s.seg_id1;
            segment.leader = s.leader;
            for b in s.blocks {
                let block = Block{
                    ino: ino,
                    generation: 0,
                    offset: b.offset,
                    seg_start_addr: b.seg_start_addr,
                    seg_end_addr: b.seg_end_addr,
                    size: b.size,
                };
                segment.blocks.push(block);
            }
            segments.push(segment);
        }

        println!("the segments of ino: {} are: {:?}", ino, segments);
        Ok(segments)
    }

    pub fn open_segment(&self, seg: &Segment) -> Errno {
        let worker = self.io_pool.get_worker(seg.seg_id0, seg.seg_id1);
        let (tx, mut rx) = mpsc::channel::<Errno>(1);
        let msg = MsgFileOpenOp{
            id0: seg.seg_id0,
            id1: seg.seg_id1,
            dir: self.data_dirs[0].clone(),
            resp_sender: tx,
        };
        let ret = worker.send_disk_io(MsgFileOp::OpOpen(msg));
        if !ret.is_success() {
            println!("open_segment(id0: {}, id1: {}): failed to send open msg, err: {:?}",
            seg.seg_id0, seg.seg_id1, ret);
            return ret;
        }
        let ret = self.exec.get_runtime().block_on(rx.recv());
        if let Some(e) = ret {
            return e;
        }
        println!("open_segment(id0: {}, id1: {}): got invalid ret", seg.seg_id0, seg.seg_id1);
        return Errno::Eintr;
    }

    pub fn write_segment(&self, seg: &mut Segment, ino: u64, offset: u64, data: &[u8]) -> Result<u32, Errno> {
        let worker = self.io_pool.get_worker(seg.seg_id0, seg.seg_id1);
        let (tx, mut rx) = mpsc::channel::<MsgFileWriteResp>(1);
        let msg = MsgFileWriteOp{
            id0: seg.seg_id0,
            id1: seg.seg_id1,
            offset: offset,
            data: data.to_vec(),
            resp_sender: tx,
        };
        let ret = worker.send_disk_io(MsgFileOp::OpWrite(msg));
        if !ret.is_success() {
            println!("write_segment: failed to send_disk_io for seg({:?}), offset: {}, err: {:?}",
            seg, offset, ret);
            return Err(Errno::Eintr);
        }
        let ret = self.exec.get_runtime().block_on(rx.recv());
        if let Some(r) = ret {
            if !r.err.is_success() {
                println!("write_segment: failed to write segment for seg({:?}), offset: {}, err: {:?}",
                seg, offset, r.err);
                return Err(r.err);
            }
            // add block into segment.
            seg.add_block(ino, offset, r.offset, r.nwrite);
            // update the meta service.
            let meta_seg = seg.to_meta_segment();
            let ret = self.meta_service_mgr.add_file_block(ino, &meta_seg);
            if !ret.is_success(){
                println!("failed to add file block for ino: {}, meta_seg: {:?}, err: {:?}",
                ino, meta_seg, ret);
                return Err(ret);
            }
            println!("write_segment: succeed to add_file_block for ino: {}, set: {:?}", ino, meta_seg);
            return Ok(r.nwrite);
        }
        println!("write_segment: got invalid response for seg({:?}, offset: {}", seg, offset);
        return Err(Errno::Eintr);
    }

    pub fn create(dirs: Vec<String>, mgr: &'a Box<dyn MetaServiceMgr>, exec: &Executor)->Box<SegmentMgr<'a>> {
        Box::new(SegmentMgr{
            meta_service_mgr: mgr,
            data_dirs: dirs,
            io_pool: IoThreadPool::new(2, exec),
            exec: exec.clone(),
        })
    }
}