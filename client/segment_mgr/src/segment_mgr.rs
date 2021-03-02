extern crate tokio;

use tokio::sync::mpsc;
use crate::types::{Segment, Block};
use common::error::Errno;
use common::runtime::Executor;
use metaservice_mgr::mgr::MetaServiceMgr;
use io_engine::io_thread_pool::IoThreadPool;
use io_engine::types::{MsgFileOpenOp, MsgFileOp};
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

        Ok(segments)
    }

    pub fn open_segment(&self, seg: &Segment) -> Errno {
        let worker = self.io_pool.get_worker(seg.seg_id0, seg.seg_id1);
        let (tx, mut rx) = mpsc::channel::<Errno>(0);
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

    pub fn create(dirs: Vec<String>, mgr: &'a Box<dyn MetaServiceMgr>, exec: &Executor)->Box<SegmentMgr<'a>> {
        Box::new(SegmentMgr{
            meta_service_mgr: mgr,
            data_dirs: dirs,
            io_pool: IoThreadPool::new(2, exec),
            exec: exec.clone(),
        })
    }
}