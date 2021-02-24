extern crate hyper;
extern crate tokio;

use std::collections::HashMap;
use std::io::Read;
use hyper::{Client, Request, Body};
use bytes::Buf as _;
use crate::runtime::Executor;

pub enum HttpMethod{
    Get,
    Put,
    Post,
    Delete,
}
#[derive (Debug, Default)]
pub struct RespText{
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: String,
}

pub struct HttpClient{
    pub retry_times: u32,
    http_client: Client<hyper::client::HttpConnector, hyper::Body>,
    exec: Executor,
}

struct Resp{
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: Box<dyn Read>,
}

impl HttpClient{
    pub fn new(retry_times: u32, exec: &Executor) -> HttpClient{
        HttpClient{
            retry_times: retry_times,
            http_client: Client::new(),
            exec: exec.clone(),
        }
    }

    pub fn request(&self, url: &String, body: &String, method: &HttpMethod) -> Result<RespText, String>{
        let mut count = self.retry_times;
        while count > 0 {
            count -= 1;
            let req : Request<Body>;
            let ret = hyper::Request::builder().
                        method(self.get_http_method(method)).
                        header("Content-Type", "application/json").
                        uri(url.clone()).
                        body(hyper::Body::from(body.clone()));
            match ret {
                Ok(ret) => {
                    req = ret;
                }
                Err(error) => {
                    return Err(format!("failed to create request from url: {}, body: {}, err: {}", url.clone(), body.clone(), error)); 
                }
            }
            let mut resp : Resp;
            let result = self.exec.get_runtime().block_on(self.send(req));
            match result {
                Ok(result) => {
                    resp = result;
                }
                Err(error) => {
                    println!("failed to send request to url: {}, err: {} in {} time", url.clone(), error, count+1); 
                    continue;
                }
            }
            let mut buf = Vec::<u8>::new();
            let ret = std::io::copy(resp.body.as_mut(), &mut buf);
            match ret {
                Ok(_n) => {
                    let bstr = String::from_utf8(buf);
                    match bstr {
                        Ok(bstr) => {
                            let rtext = RespText{
                                status: resp.status,
                                headers: resp.headers,
                                body: bstr,
                            };
                            return Ok(rtext);
                        }
                        Err(error) => {
                            return Err(format!("got invalid body with error: {}", error));
                        }
                    } 
                }
                Err(error) => {
                    return Err(format!("failed to read body from {}, err: {}", url.clone(), error)); 
                }
            }
        }

        return Err(format!("failed to send request to url {} with body {} in {} times", url.clone(), body.clone(), self.retry_times));
    }

    fn get_http_method(&self, m: &HttpMethod) -> hyper::Method {
        match m {
            HttpMethod::Put => {
                hyper::Method::PUT
            }
            HttpMethod::Post => {
                hyper::Method::POST
            }
            HttpMethod::Delete => {
                hyper::Method::DELETE
            }
            _ => {
                hyper::Method::GET
            }
        }
    }
    
    async fn send(&self, req: hyper::Request<hyper::Body>) -> Result<Resp, String>{
        // use the http2
        //*req.version_mut() = hyper::Version::HTTP_2;
        let resp = self.http_client.request(req).await;
        match resp {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let mut headers = HashMap::new();
                for (h,v) in resp.headers(){
                    let v = std::str::from_utf8(v.as_bytes());
                    match v {
                        Ok(v) => {
                            headers.insert(String::from(h.as_str()), String::from(v));
                        }
                        Err(error) =>{
                            println!("got invalid header {} with error: {}", h.as_str(), error);
                        }
                    }
                    
                }
                let body = hyper::body::aggregate(resp).await;
                match body {
                    Ok(body) => {
                        let result = Resp{
                            status: status,
                            headers: headers,
                            body: Box::new(body.reader()),
                        };
                        return Ok(result);
                    }
                    Err(error) => {
                        return Err(format!("failed to get body, err: {}", error));
                    }
                }
            }
            Err(error) => {
                return Err(format!("http request is failed with err: {}", error));
            }
        }
    }
}