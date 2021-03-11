extern crate tokio;

use crate::types::{Block, Segment, SegmentIo};
use crate::leader_mgr::LeaderMgr;
use common::error::Errno;
use common::runtime::Executor;
use metaservice_mgr::mgr::MetaServiceMgr;

pub struct SegmentMgr<'a> {
    meta_service_mgr: &'a Box<dyn MetaServiceMgr>,
    data_dirs: Vec<String>,
    leader_mgr: LeaderMgr,
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
        let leader = self.leader_mgr.get_leader(&seg.leader);
        let seg_io = SegmentIo{
            id0: seg.seg_id0,
            id1: seg.seg_id1,
            dir: self.data_dirs[0].clone(),
        };
        leader.open(&seg_io)
    }

    pub fn write_segment(&self, seg: &mut Segment, ino: u64, offset: u64, data: &[u8]) -> Result<u32, Errno> {
        let seg_io = SegmentIo{
            id0: seg.seg_id0,
            id1: seg.seg_id1,
            dir: self.data_dirs[0].clone(),
        };
        let leader = self.leader_mgr.get_leader(&seg.leader);
        let ret = leader.write(&seg_io, data);
        match ret {
            Ok(ret) => {
                let nwrite = ret.size;
                seg.add_block(ino, offset, ret.offset, nwrite);
                // update the meta service.
                let meta_seg = seg.to_meta_segment();
                let ret = self.meta_service_mgr.add_file_block(ino, &meta_seg);
                if !ret.is_success(){
                    println!("failed to add file block for ino: {}, meta_seg: {:?}, err: {:?}",
                    ino, meta_seg, ret);
                    return Err(ret);
                }
                println!("write_segment: succeed to add_file_block for ino: {}, set: {:?},  nwrite: {}", 
                    ino, meta_seg, nwrite);
                return Ok(nwrite);
            }
            Err(err) => {
                println!("write_segment: failed to write({:?}) for ino: {}, offset: {}",
                seg_io, ino, offset);
                return Err(err);
            }
        }
    }

    pub fn create(dirs: Vec<String>, mgr: &'a Box<dyn MetaServiceMgr>, exec: &Executor)->Box<SegmentMgr<'a>> {
        Box::new(SegmentMgr{
            meta_service_mgr: mgr,
            data_dirs: dirs,
            leader_mgr: LeaderMgr::new(mgr.get_machine_id(), 2, exec),
        })
    }
}