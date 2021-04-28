
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::cmp::{Eq, PartialEq};

#[derive(Copy, Clone)]
pub enum SegState{
    Unknown = 0,
    CacheOpen,
    CacheRead,
    CacheWrite,
    CacheReadEof,
    CacheClose,
    BackendRead,
    BackendReadEof,
    BackendWrite,
    MetaUpload,
}

// impl trait Eq
impl PartialEq for SegState{
    fn eq(&self, other: &Self) -> bool {
        self.u8() == other.u8()
    }
}

impl Eq for SegState{}

// impl trait Hash
impl Hash for SegState{
    fn hash<H: Hasher>(&self, state: &mut H){
        self.u8().hash(state);
    }
}

impl SegState {
    pub fn u8(&self) -> u8{
        *self as u8
    }
}

pub struct SegStateMachine{
    id0: u64,
    id1: u64,
    dir: String,
    offset: u64, // records the offset to read/write for the segment.
    current_state: SegState,
    state_machine: HashMap<SegState, SegState>,
}

impl SegStateMachine{
    pub fn new(id0: u64, id1: u64, dir: &String) -> Self{
        SegStateMachine{
            id0: id0,
            id1: id1,
            dir: dir.clone(),
            offset: 0,
            current_state: SegState::Unknown,
            state_machine: HashMap::new(),
        }
    }

    pub fn prepare_for_upload(&mut self){
        self.state_machine.insert(SegState::CacheOpen, SegState::CacheRead);
        self.state_machine.insert(SegState::CacheRead, SegState::BackendWrite);
        self.state_machine.insert(SegState::BackendWrite, SegState::MetaUpload);
        self.state_machine.insert(SegState::CacheReadEof, SegState::CacheClose);
    }
    
    pub fn prepare_for_download(&mut self){
        self.state_machine.insert(SegState::CacheOpen,SegState::BackendRead);
        self.state_machine.insert(SegState::BackendRead, SegState::CacheWrite);
        self.state_machine.insert(SegState::BackendReadEof, SegState::CacheClose);
    }

    pub fn set_state(&mut self, state: SegState){
        self.current_state = state;
    }

    pub fn set_offset(&mut self, offset:u64) {
        self.offset = offset;
    }

    pub fn get_offset(&self) -> u64{
        self.offset
    }

    pub fn get_dir(&self) -> &String{
        &self.dir
    }
    pub fn get_next_state(&self) -> SegState {
        if let Some(s) = self.state_machine.get(&self.current_state){
            return *s;
        }
        return SegState::Unknown;
    }
}