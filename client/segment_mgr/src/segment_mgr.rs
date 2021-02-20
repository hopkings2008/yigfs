
use crate::types::{Segment, Block};
use common::error::Errno;
use metaservice_mgr::mgr::MetaServiceMgr;
pub struct SegmentMgr<'a> {
    meta_service_mgr: &'a Box<dyn MetaServiceMgr>,
}

impl<'a> SegmentMgr<'a> {
    pub fn get_file_segments(&self, ino: u64)-> Result<Vec<Segment>, Errno> {
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

    pub fn create(mgr: &'a Box<dyn MetaServiceMgr>)->Box<SegmentMgr> {
        Box::new(SegmentMgr{
            meta_service_mgr: mgr,
        })
    }
}