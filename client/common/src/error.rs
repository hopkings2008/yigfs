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
    // not space left
    Enospc = 28,
    // access denied
    Eaccess = 9,
    // range error
    Erange = 10,
    // offset err
    Eoffset = 11,
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

    pub fn is_enospc(&self) -> bool {
        match *self {
            Errno::Enospc => {
                true
            }
            _ => {
                false
            }
        }
    }

    pub fn is_enotf(&self) -> bool {
        match *self {
            Errno::Enotf => {
                true
            }
            _ => {
                false
            }
        }
    }

    pub fn is_bad_offset(&self) -> bool {
        match *self {
            Errno::Eoffset => {
                true
            }
            _ => {
                false
            }
        }
    }
}
