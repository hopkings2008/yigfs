extern crate hyper;
extern crate hyper_tls;
extern crate tokio;

use std::collections::HashMap;
use std::collections::BTreeMap;

use bytes::Buf;
use bytes::Bytes;
use hyper::{Client, Request, Body};
use hyper::client::HttpConnector;
use hyper_tls::HttpsConnector;
use http::header::{HeaderName, HeaderValue};
use log::error;


pub enum HttpMethod{
    Get,
    Put,
    Post,
    Delete,
    Head,
}
#[derive (Debug, Default)]
pub struct RespText{
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: String,
}

pub struct HttpClient{
    pub retry_times: u32,
    http_client: Client<HttpConnector, hyper::Body>,
    https_client: Client<hyper_tls::HttpsConnector<HttpConnector>, hyper::Body>,
    pub headers: BTreeMap<String, Vec<Vec<u8>>>,
}

struct Resp{
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

impl HttpClient{
    pub fn new(retry_times: u32) -> HttpClient{
        let https = HttpsConnector::new();
        HttpClient{
            retry_times: retry_times,
            http_client: Client::new(),
            https_client: Client::builder().build::<_, hyper::Body>(https),
            headers: BTreeMap::new(),
        }
    }

    pub fn set_headers(&mut self, headers: BTreeMap<String, Vec<Vec<u8>>>) {
        self.headers = headers
    }

    pub async fn request(&self, url: &String, body: &[u8], method: &HttpMethod, is_v4: bool) -> Result<RespText, String>
    {
        let mut count = self.retry_times;
        while count > 0 {
            count -= 1;
            let mut req : Request<Body>;
            let ret = hyper::Request::builder().
                        method(self.get_http_method(method)).
                        header("Content-Type", "application/json").
                        uri(url.clone()).
                        body(hyper::Body::from(Bytes::copy_from_slice(body)));
            match ret {
                Ok(ret) => {
                    req = ret;
                    // use http2
                    //*(req.version_mut()) = hyper::Version::HTTP_2;
                    if is_v4 {
                        if self.headers.is_empty() {
                            return Err(format!("V4 request headers is not None"));
                        }

                        for h in self.headers.iter() {
                            // add header
                            let header_name = match h.0.parse::<HeaderName>() {
                                Ok(name) => name,
                                Err(err) => {
                                    return Err(format!("error parsing header name: {}", err));
                                }
                            };

                            for v in h.1.iter() {
                                let header_value = match HeaderValue::from_bytes(v) {
                                    Ok(value) => value,
                                    Err(err) => {
                                        return Err(format!("error parsing header value: {}", err));
                                    }
                                };

                                req.headers_mut().insert(&header_name, header_value);
                            }
                        }
                    } else {
                        let header_value = HeaderValue::from_bytes(b"application/json").unwrap();
                        req.headers_mut().insert("Content-Type", header_value);
                    }
                }
                Err(error) => {
                    return Err(format!("failed to create request from url: {}, body: {:?}, err: {}", url.clone(), body.clone(), error)); 
                }
            }
            let resp : Resp;
            let result = self.send(req).await;
            match result {
                Ok(result) => {
                    resp = result;
                }
                Err(error) => {
                    error!("failed to send request to url: {}, err: {} in {} time", url.clone(), error, count+1); 
                    continue;
                }
            }
            
            let bstr = String::from_utf8(resp.body);
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

        return Err(format!("failed to send request to url {} with body {:?} in {} times", url.clone(), body.clone(), self.retry_times));
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
            HttpMethod::Head => {
                hyper::Method::HEAD
            }
            _ => {
                hyper::Method::GET
            }
        }
    }
    
    async fn send(&self, req: hyper::Request<hyper::Body>) -> Result<Resp, String>{
        let mut is_https = false;
        if let Some(s) = req.uri().scheme() {
            if s.as_str() == "https" {
                is_https = true;
            }
        }
        let resp: Result<hyper::Response<Body>, hyper::Error>;
        if is_https {
            resp = self.https_client.request(req).await;
        } else {
            resp = self.http_client.request(req).await;
        }
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
                            error!("got invalid header {} with error: {}", h.as_str(), error);
                        }
                    }
                    
                }
                let body = hyper::body::aggregate(resp).await;
                match body {
                    Ok(mut body) => {
                        let mut data: Vec<u8> = Vec::new();
                        while body.remaining() > 0 {
                            let s: usize;
                            {
                                let d = body.chunk();
                                data.append(&mut d.to_vec());
                                s = d.len();
                            }
                            body.advance(s);
                        }
                        let result = Resp{
                            status: status,
                            headers: headers,
                            body: data,
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