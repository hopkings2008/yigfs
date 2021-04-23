
use std::collections::HashMap;
use std::rc::Rc;

use common::runtime::Executor;
use io_engine::backend_storage::BackendStore;
use io_engine::cache_store::CacheStore;

use crate::{leader::Leader, segment_mgr::SegmentMgr};
use crate::leader_local::LeaderLocal;
use crate::leader_not_support::LeaderNotSupport;

#[derive(Debug)]
enum LeaderType {
    Local = 0,
    Peer = 1,
    Unknown = 2,
}
pub struct LeaderMgr {
    machine: String,
    leaders: HashMap<u8, Box<dyn Leader>>,
}

impl LeaderMgr {
    pub fn new(machine: &String, exec: &Executor, seg_mgr: Rc<SegmentMgr>, 
        cache_store: Box<dyn CacheStore>, backend_store: Box<dyn BackendStore>) -> Self {
        let mut leaders = HashMap::<u8, Box<dyn Leader>>::new();
        leaders.insert(LeaderType::Unknown as u8, Box::new(LeaderNotSupport::new()));
        leaders.insert(LeaderType::Local as u8, Box::new(LeaderLocal::new(machine,  exec, seg_mgr, cache_store, backend_store)));
        LeaderMgr{
            machine: machine.clone(),
            leaders: leaders,
        }
    }
    pub fn stop(&mut self){
        for (k, l) in &mut self.leaders {
            l.release();
            println!("leader of {:?} is stopped.", k);
        }
        self.leaders.clear();
    }
    pub fn get_leader(&self, leader: &String) -> &Box<dyn Leader> {
        let mut leader_type = LeaderType::Unknown as u8;
        if *leader == self.machine {
            leader_type = LeaderType::Local as u8;
        } else if !leader.is_empty(){// peer is not supported yet.
            leader_type = LeaderType::Peer as u8;
        }

        // will not crash here, because unknown is always in the hashmap.
        self.leaders.get(&leader_type).unwrap()
    }
}