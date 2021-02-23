#[derive(Debug)]
pub enum Errno{
    // success
    Esucc = 0,
    // internal error.
    Eintr = 1,
    // no more items.
    Enoent = 2,
    // already existing.
    Eexists = 3,
}

impl Errno {
    pub fn is_exists(&self)->bool {
        match *self {
            Errno::Eexists => {
                true
            }
            _ => {
                false
            }
        }
    }
    
    pub fn is_success(&self) -> bool {
        match *self {
            Errno::Esucc =>{
                true
            }
            _ => {
                false
            }
        }
    }
}