
use std::collections::HashMap;

use common::runtime::Executor;

use crate::leader::Leader;
use crate::leader_local::LeaderLocal;
use crate::leader_not_support::LeaderNotSupport;

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
    pub fn new(machine: &String, thr_num: u32, exec: &Executor) -> Self {
        let mut leaders = HashMap::<u8, Box<dyn Leader>>::new();
        leaders.insert(LeaderType::Unknown as u8, Box::new(LeaderNotSupport::new()));
        leaders.insert(LeaderType::Local as u8, Box::new(LeaderLocal::new(thr_num, exec)));
        LeaderMgr{
            machine: machine.clone(),
            leaders: leaders,
        }
    }
    pub fn get_leader(&self, leader: &String) -> &Box<dyn Leader> {
        let mut leader_type = LeaderType::Unknown as u8;
        if *leader == self.machine {
            leader_type = LeaderType::Local as u8;
        }

        // will not crash here, because unknown is always in the hashmap.
        self.leaders.get(&leader_type).unwrap()
    }
}