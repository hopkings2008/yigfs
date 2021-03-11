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
    // seek error
    Eseek = 4,
    // eof
    Eeof = 5,
    // not found
    Enotf = 6,
    // not support
    Enotsupp = 7,
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

    pub fn is_enoent(&self) -> bool {
        match *self{
            Errno::Enoent => {
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

    pub fn is_eof(&self) -> bool {
        match *self {
            Errno::Eeof => {
                true
            }
            _ => {
                false
            }
        }
    }
}