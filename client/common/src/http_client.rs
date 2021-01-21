extern crate hyper;

use std::collections::HashMap;
use std::io::Read;
use hyper::Client;
use bytes::Buf as _;
use tokio::runtime::Runtime;

#[derive (Debug, Default)]
pub struct RespText{
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: String,
}

pub struct HttpClient{
    pub retry_times: u32,
    http_client: Client<hyper::client::HttpConnector, hyper::Body>,
}

struct Resp{
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: Box<dyn Read>,
}

impl HttpClient{
    pub fn new(retry_times: u32) -> HttpClient{
        HttpClient{
            retry_times: retry_times,
            http_client: Client::new(),
        }
    }

    pub fn get(&self, url: &String, body: &String) -> Result<RespText, String>{
        let mut count = self.retry_times;
        while count > 0 {
            count -= 1;
            let req = hyper::Request::builder().
                        method(hyper::Method::GET).
                        uri(url.clone()).
                        body(hyper::Body::from(body.clone()));
            match req {
                Ok(req) => {
                    let result = Runtime::new()
                    .expect("Failed to create Tokio runtime").block_on(self.send(req));
                    match result {
                        Ok(mut result) => {
                            let mut buf: Vec<u8> = vec![];
                            let n = std::io::copy(result.body.as_mut(), &mut buf);
                            match n {
                                Ok(_n) => {
                                    let bstr = String::from_utf8(buf);
                                    match bstr {
                                        Ok(bstr) => {
                                            let rtext = RespText{
                                                status: result.status,
                                                headers: result.headers,
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
                        Err(error) => {
                            println!("failed to send request to url: {}, err: {} in {} time", url.clone(), error, count+1);
                        }
                    }
                }
                Err(error) => {
                    return Err(format!("failed to create request from url: {}, body: {}, err: {}", url.clone(), body.clone(), error));
                }
            }
        }

        return Err(format!("failed to send request to url {} with body {} in {} times", url.clone(), body.clone(), self.retry_times));
        
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