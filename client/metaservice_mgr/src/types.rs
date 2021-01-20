pub enum FileType {
    UNKNOWN = 0,
    FILE = 1,
    DIR = 2,
    LINK = 3,
}

impl From<u8> for FileType {
    fn from(u: u8) -> FileType {
        match u {
            1 => {
                FileType::FILE
            }
            2 => {
                FileType::DIR
            }
            3 => {
                FileType::LINK
            }
            _ => {
                FileType::UNKNOWN
            }
        }
    }
}

pub struct DirEntry{
    pub ino: u64,
    pub file_type: FileType,
    pub name: String,
}