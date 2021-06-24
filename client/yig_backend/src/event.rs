use common::error::Errno;

#[derive(Debug)]
pub struct ReqHeadObject{
    pub bucket: String,
    pub object: String,
}

#[derive(Debug)]
pub struct RespHeadObject{
    pub bucket: String,
    pub object: String,
    pub result: EventResult,
    pub size: u64,
}

#[derive(Debug)]
pub struct ReqAppendObject{
    pub bucket: String,
    pub object: String,
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub struct RespAppendObject{
    pub bucket: String,
    pub object: String,
    pub result: EventResult,
}

#[derive(Debug)]
pub struct ReqGetObject{
    pub bucket: String,
    pub object: String,
    pub offset: u64,
    pub size: u64,
}

#[derive(Debug)]
pub struct RespGetObject{
    pub bucket: String,
    pub object: String,
    pub result: EventResult,
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub struct ReqDelObject{
    pub bucket: String,
    pub object: String,
}

#[derive(Debug)]
pub struct RespDelObject{
    pub bucket: String,
    pub object: String,
    pub result: EventResult,
}

#[derive(Debug)]
pub struct EventResult{
    pub err: Errno,
    pub msg: String,
}


#[derive(Debug)]
pub enum IoEvent{
    IoHead(ReqHeadObject),
    IoAppend(ReqAppendObject),
    IoGet(ReqGetObject),
    IoDel(ReqDelObject),
}

#[derive(Debug)]
pub enum IoEventResult{
    IoHeadResult(RespHeadObject),
    IoAppendResult(RespAppendObject),
    IoGetResult(RespGetObject),
    IoDelResult(RespDelObject),
}