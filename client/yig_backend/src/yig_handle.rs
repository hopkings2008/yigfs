

pub struct Ref{
    rcount: u32,
}

impl Ref{
    pub fn new()->Self{
        Ref{
            rcount: 1,
        }
    }

    pub fn get(&mut self) -> u32 {
        self.rcount += 1;
        self.rcount
    }

    pub fn put(&mut self) -> u32 {
        self.rcount -= 1;
        self.rcount
    }
}

pub struct YigHandle {
    pub id0: u64,
    pub id1: u64,
    pub capacity: u64,
    pub size: u64,
    r: Ref,
}

impl YigHandle{
    pub fn new(id0: u64, id1: u64) -> Self{
        YigHandle{
            id0: id0,
            id1: id1,
            capacity: 0,
            size: 0,
            r: Ref::new(),
        }
    }

    pub fn get(&mut self) -> u32 {
        self.r.get()
    }

    pub fn put(&mut self) -> u32 {
        self.r.put()
    }
}