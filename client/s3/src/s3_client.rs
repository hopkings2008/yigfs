use std::io::prelude::*;
use std::fs::File;
use std::io::BufReader;
use std::convert::From;
use crate::signature::AwsCredentials;
use crate::signature::SignedRequest;
use crate::types::S3ObjectInfo;
use crate::types::AppendS3ObjectResp;

use common::http_client::HttpClient;
use common::http_client::HttpMethod;
use common::error::Errno;

pub struct S3Client {
    // region
    pub region: String,
    // endpoint
    pub endpoint: String,
    // ak
    pub ak: String,
    // sk
    pub sk: String,
}

impl S3Client {
    pub fn new(region: &str, endpoint: &str, ak: &str, sk: &str) -> S3Client {
        S3Client {
            region: region.to_string(),
            endpoint: endpoint.to_string(),
            ak : ak.to_string(),
            sk: sk.to_string(),
        }
    }

    pub async fn head_object(&self, bucket: &str, object: &str) -> Result<S3ObjectInfo, Errno>{
        let path = format!("/{}/{}", bucket, object);

        // create url
        let url = String::from("http://") + &self.endpoint + &path;

        let body = Vec::new();
        let aws_credentials = AwsCredentials::new(&self.ak, &self.sk);
        //sign the request
        let mut request = SignedRequest::new("HEAD", "s3", &self.region, &path, &self.endpoint);
        request.sign(&aws_credentials, &body);

        // set the head object req header, then send it.
        let retry_times = 3;
        let mut client = HttpClient::new(retry_times);
        client.set_headers(request.headers);
        
        let resp = client.request(&url, &body, &HttpMethod::Head, true).await;
        match resp {
            Ok(resp) => {
                if resp.status == 403 {
                    return Err(Errno::Eaccess)
                } else if resp.status == 404 {
                    return Err(Errno::Enotf)
                } else if resp.status >= 300 {
                    println!("Failed to head object, resp status is: {}, body is: {}", resp.status, resp.body);
                    return Err(Errno::Eintr)
                }

                let object_length = resp.headers.get("content-length");
                match object_length {
                    Some(size) => {
                        let rtext = S3ObjectInfo {
                            bucket: bucket.to_string(),
                            name: object.to_string(),
                            size: size.parse::<u64>().unwrap(),
                        };
                        return Ok(rtext);
                    }
                    None => {
                        println!("Err object length is none.");
                        return Err(Errno::Eintr)
                    }
                }
            }
            Err(_) => {
                return Err(Errno::Eintr)
            }
        }
    }

    pub async fn append_object_by_path(&self, bucket: &str, object: &str, append_position: &u64, object_path: &str) -> Result<AppendS3ObjectResp, Errno> {
        // add the body
        let f = File::open(object_path).expect("Error to open the object file");
        let mut reader = BufReader::new(f);
        let mut body = Vec::new();
        reader.read_to_end(&mut body).expect("err to read the object file");

        // create url
        let params = String::from("?append");
        let path = format!("/{}/{}", bucket, object);
        let url = String::from("http://") + &self.endpoint + &path + &params;
        // add the position to url
        let final_url = &format!("{}&position={}", url, append_position);

        let mut request = SignedRequest::new("POST", "s3", &self.region, &path, &self.endpoint);

        // add the params
        request.add_param("append", "");
        request.add_param("position", &append_position.to_string());

        //sign the request
        let aws_credentials = AwsCredentials::new(&self.ak, &self.sk);
        request.sign(&aws_credentials, &body);

        // set the append object req header, then send it.
        let retry_times = 3;
        let mut client = HttpClient::new(retry_times);
        client.set_headers(request.headers);
        
        let resp = client.request(&final_url, &body, &HttpMethod::Post, true).await;
        match resp {
            Ok(resp) => {
                if resp.status == 403 {
                    return Err(Errno::Eaccess)
                } else if resp.status >= 300 {
                    println!("Failed to append object by path, resp status is: {}, body is: {}", resp.status, resp.body);
                    return Err(Errno::Eintr)
                }

                let next_append_position = resp.headers.get("x-amz-next-append-position");
                match next_append_position {
                    Some(position) => {
                        let rtext = AppendS3ObjectResp {
                            bucket: bucket.to_string(),
                            name: object.to_string(),
                            next_append_position: position.parse::<u64>().unwrap(),
                        };
                        return Ok(rtext);
                    }
                    None => {
                        println!("Err next append object position is None.");
                        return Err(Errno::Eintr)
                    }
                }
            }
            Err(_) => {
                return Err(Errno::Eintr)
            }
        }
    }

    pub async fn append_object(&self, bucket: &str, object: &str, append_position: &u64, data: &[u8]) -> Result<AppendS3ObjectResp, Errno> {
        // create url
        let params = String::from("?append");
        let path = format!("/{}/{}", bucket, object);
        let url = String::from("http://") + &self.endpoint + &path + &params;
        // add the position to url
        let final_url = &format!("{}&position={}", url, append_position);

        let mut request = SignedRequest::new("POST", "s3", &self.region, &path, &self.endpoint);

        // add the params
        request.add_param("append", "");
        request.add_param("position", &append_position.to_string());

        //sign the request
        let aws_credentials = AwsCredentials::new(&self.ak, &self.sk);
        request.sign(&aws_credentials, data);

        // set the append object req header, then send it.
        let retry_times = 3;
        let mut client = HttpClient::new(retry_times);
        client.set_headers(request.headers);

        let resp = client.request(&final_url, data, &HttpMethod::Post, true).await;
        match resp {
            Ok(resp) => {
                if resp.status == 403 {
                    return Err(Errno::Eaccess)
                } else if resp.status >= 300 {
                    println!("Failed to append object, resp status is: {}, body is: {}", resp.status, resp.body);
                    return Err(Errno::Eintr)
                }

                let next_append_position = resp.headers.get("x-amz-next-append-position");
                match next_append_position {
                    Some(position) => {
                        let rtext = AppendS3ObjectResp {
                            bucket: bucket.to_string(),
                            name: object.to_string(),
                            next_append_position: position.parse::<u64>().unwrap(),
                        };
                        return Ok(rtext);
                    }
                    None => {
                        println!("Err next append object position is None.");
                        return Err(Errno::Eintr)
                    }
                }
            }
            Err(_) => {
                return Err(Errno::Eintr)
            }
        }
    }

    pub async fn get_object(&self, bucket: &str, object: &str, offset: &u64, size: &u32) -> Result<Vec<u8>, Errno>{
        let path = format!("/{}/{}", bucket, object);

        // create url
        let url = String::from("http://") + &self.endpoint + &path;

        let body = Vec::new();
        let aws_credentials = AwsCredentials::new(&self.ak, &self.sk);
        //sign the request
        let mut request = SignedRequest::new("GET", "s3", &self.region, &path, &self.endpoint);
        request.sign(&aws_credentials, &body);

        // add range header
        let get_size: u64 = From::from(size.clone());
        let end = offset + get_size - 1;
        request.add_header("Range", &format!("bytes={}-{}", offset, end));

        // set the get object req header, then send it.
        let retry_times = 3;
        let mut client = HttpClient::new(retry_times);
        client.set_headers(request.headers);
        
        let resp = client.request(&url, &body, &HttpMethod::Get, true).await;
        match resp {
            Ok(resp) => {
                if resp.status == 403 {
                    return Err(Errno::Eaccess)
                } else if resp.status == 404 {
                    return Err(Errno::Enotf)
                } else if resp.status == 416 {
                    return Err(Errno::Erange)
                } else if resp.status >= 300 {
                    println!("Failed to get object, resp status is: {}, body is: {}", resp.status, resp.body);
                    return Err(Errno::Eintr)
                }

                let object = resp.body.into_bytes();
                return Ok(object);
            }
            Err(_) => {
                return Err(Errno::Eintr)
            }
        }
    }
}
