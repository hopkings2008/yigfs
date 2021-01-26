#[derive(Debug)]
pub enum Errno{
    // success
    Esucc = 0,
    // internal error.
    Eintr = 1,
    // no more items.
    Enoent = 2,
}