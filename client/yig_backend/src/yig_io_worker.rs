use common::runtime::Executor;
use common::error::Errno;
use io_engine::types::{MsgFileOp, MsgFileOpenResp, MsgFileReadData, MsgFileWriteResp, MsgFileOpResp};
use io_engine::io_worker::{IoWorker, IoWorkerFactory};
use s3::s3_client::S3Client;
use crossbeam_channel::{Receiver, select};


#[derive(Debug, Default)]
pub struct S3Config{
    pub region: String,
    pub endpoint: String,
    pub ak: String,
    pub sk: String,
}

//initial proactor, trigger the async io to yig.
pub struct YigIoWorker {
    op_rx: Receiver<MsgFileOp>,
    stop_rx: Receiver<u8>,
    exec: Executor,
    s3_client: S3Client,
}

impl IoWorker for YigIoWorker {
    fn start(&mut self){
        loop{
            select! {
                recv(self.op_rx) -> msg => {
                    match msg {
                        Ok(msg) => {
                            self.do_work(&msg);
                        }
                        Err(err) => {
                            println!("YigIoWorker: failed to recv op message, err: {}", err);
                            break;
                        }
                    }
                }
                recv(self.stop_rx) -> msg => {
                    match msg {
                        Ok(msg) => {
                            println!("YigIoWorker: got stop message: {:?}", msg);
                            break;
                        }
                        Err(err) => {
                            println!("YigIoWorker: failed to recv stop message: err: {}", err);
                            break;
                        }
                    }
                }
            }
        }
    }
}

impl YigIoWorker{
    fn new(op_rx: Receiver<MsgFileOp>, stop_rx: Receiver<u8>, s3_config: &S3Config) -> Self{
        YigIoWorker{
            op_rx: op_rx,
            stop_rx: stop_rx,
            exec: Executor::create_single_threaded(),
            s3_client: S3Client::new(s3_config.region.as_str(),
            s3_config.endpoint.as_str(), s3_config.ak.as_str(), s3_config.sk.as_str()),
        }
    }

    fn id_to_object_name(&self, id0: u64, id1: u64) -> String {
        format!("{}_{}.seg", id0, id1)
    }

    fn do_work(&self, msg: &MsgFileOp){
        match msg {
            MsgFileOp::OpOpen(msg_open) => {
                let obj = self.id_to_object_name(msg_open.id0, msg_open.id1);
                let mut result = Errno::Esucc;
                let ret = self.open(&msg_open.dir, &obj);
                match ret {
                    Ok(ret) => {
                        println!("open yig object: {}/{}, size: {}", msg_open.dir, obj, ret);
                    }
                    Err(err) => {
                        println!("failed to open yig object: {}/{}, err: {:?}", msg_open.dir, obj, err);
                        result = err;
                    }
                }
                let ret = msg_open.resp_sender.send(MsgFileOpResp::OpRespOpen(MsgFileOpenResp{
                    id0: msg_open.id0,
                    id1: msg_open.id1,
                    err: result,
                }));
                match ret {
                    Ok(_) => {
                    }
                    Err(err) => {
                        println!("open yig object: failed to send open result, err: {}", err);
                    }
                }
            }

            MsgFileOp::OpRead(msg_read) => {
                let obj = self.id_to_object_name(msg_read.id0, msg_read.id1);
                let mut resp = MsgFileReadData{
                    data: None,
                    err: Errno::Eintr,
                };
                let ret = self.read(&msg_read.dir,
                    &obj,
                    msg_read.offset,
                    msg_read.size);
                match ret {
                    Ok(ret) =>{
                        resp.data = Some(ret);
                    }
                    Err(err) => {
                        println!("YigIoWorker: OpRead: failed to read: {}/{}, offset: {}, size: {}, err: {:?}",
                    msg_read.dir, obj, msg_read.offset, msg_read.size, err);
                        resp.err = err;
                    }
                }
                msg_read.response(resp);
            }

            MsgFileOp::OpWrite(msg_write) => {
                let obj = self.id_to_object_name(msg_write.id0, msg_write.id1);
                let mut resp = MsgFileWriteResp{
                    offset: 0,
                    nwrite: 0,
                    err: Errno::Esucc,
                };
                let ret = self.write(&msg_write.dir, &obj, msg_write.offset,
                     msg_write.data.as_slice());
                match ret {
                    Ok(ret) => {
                        resp.offset = ret - msg_write.data.len() as u64;
                        resp.nwrite = msg_write.data.len() as u32;
                    }
                    Err(err) => {
                        println!("failed to write to yig for {}/{}, offset: {}, data len: {}, err: {:?}",
                        msg_write.dir, obj, msg_write.offset, msg_write.data.len(), err);
                        resp.err = err;
                    }
                }
                let ret = msg_write.resp_sender.send(MsgFileOpResp::OpRespWrite(resp));
                match ret{
                    Ok(_) => {}
                    Err(err) => {
                        println!("failed to write to yig: failed send write resp for {}/{}, offset: {}, data_len: {}, err: {:?}",
                    msg_write.dir, obj, msg_write.offset, msg_write.data.len(), err);
                    }
                }
            }
            MsgFileOp::OpClose(msg_close) => {
                println!("close: id0: {}, id1: {}", msg_close.id0, msg_close.id1);
            }
        }
    }

    fn open(&self, bucket: &String, object: &String)->Result<u64, Errno>{
        let ret = self.exec.get_runtime().
        block_on(self.s3_client.head_object(bucket, object));
        match ret {
            Ok(ret) => {
                return Ok(ret.size);
            }
            Err(err) => {
                if err.is_enotf() {
                    println!("{}/{} doesn't exist", bucket, object);
                    return Ok(0);
                }
                println!("failed to head {}/{}, err: {:?}", bucket, object, err);
                return Err(err);
            }
        }
    }

    fn write(&self, bucket: &String, object: &String, offset: u64, data: &[u8]) -> Result<u64, Errno>{
        let ret = self.exec.get_runtime().
        block_on(self.s3_client.append_object(bucket, object, &offset, data));
        match ret {
            Ok(ret) => {
                return Ok(ret.next_append_position);
            }
            Err(err) => {
                println!("failed to append({}/{}, offset: {}, size: {}, err: {:?}",
                bucket, object, offset, data.len(), err);
                return Err(err);
            }
        }
    }

    fn read(&self, bucket: &String, object: &String, offset: u64, size: u32) -> Result<Vec<u8>, Errno> {
        let ret = self.exec.get_runtime().block_on(
            self.s3_client.get_object(&bucket, &object, &offset, &size)
        );
        match ret {
            Ok(ret) => {
                return Ok(ret);
            }
            Err(err) => {
                println!("failed to get({}/{}, offset: {}, size: {}, err: {:?}",
                bucket, object, offset, size, err);
                return Err(err);
            }
        }
    }
}

pub struct YigIoWorkerFactory{
    s3_cfg: S3Config,
}

impl IoWorkerFactory for YigIoWorkerFactory {
    fn new_worker(&self, _exec: &Executor, op_rx: Receiver<MsgFileOp>, stop_rx: Receiver<u8>)->Box<dyn IoWorker + Send>{
        Box::new(YigIoWorker::new(op_rx, stop_rx, &self.s3_cfg))
    }
}

impl YigIoWorkerFactory{
    pub fn new(region: &String, endpoint: &String, ak: &String, sk: &String)->Box<dyn IoWorkerFactory>{
        Box::new(YigIoWorkerFactory{
            s3_cfg: S3Config{
                region: region.clone(),
                endpoint: endpoint.clone(),
                ak: ak.clone(),
                sk: sk.clone(),
           },
        })
    }
}