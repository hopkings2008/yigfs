pub mod signature;
pub mod http_v4_client;

use std::io::prelude::*;
use std::fs::File;
use std::io::BufReader;
use signature::AwsCredentials;
use signature::SignedRequest;

use http_v4_client::HttpV4Client;
use common::runtime::Executor;
use common::http_client::RespText;


pub struct S3Client {
    // region
    pub region: String,
    // host
    pub host: String,
    // bucket
    pub bucket: String,
    // object
    pub object: String,
    // ak
    pub ak: String,
    // sk
    pub sk: String,
    // body
    pub object_path: String,
    // append object position
    pub append_position: u128,
}

impl S3Client {
    pub fn new(region: &str, host: &str, bucket: &str, object: &str, ak: &str, sk: &str, object_path: &str, append_position: u128) -> S3Client {
        S3Client {
            region: region.to_string(),
            host: host.to_string(),
            bucket: bucket.to_string(),
            object: object.to_string(),
            ak : ak.to_string(),
            sk: sk.to_string(),
            object_path: object_path.to_string(),
            append_position: append_position,
        }
    }

    pub fn append_object(&self) -> Result<RespText, String> {
        // add the body
        let f = File::open(&self.object_path).expect("Error to open the object file");
        let mut reader = BufReader::new(f);
        let mut body = Vec::new();
        reader.read_to_end(&mut body).expect("err to read the object file");

        // create url
        let params = String::from("?append");
        let path = format!("/{}/{}", self.bucket, self.object);
        let url = String::from("http://") + &self.host + &path + &params;
        // add the position to url
        let final_url = &format!("{}&position={}", url, self.append_position);

        let mut request = SignedRequest::new("POST", "s3", &self.region, &path, &self.host);

        // add the params
        request.add_param("append", "");
        request.add_param("position", &self.append_position.to_string());

        //sign the request
        let aws_credentials = AwsCredentials::new(&self.ak, &self.sk);
        request.sign(&aws_credentials, &body);

        // send append object req
        let retry_times = 3;
        let exec = Executor::create();
        let client = HttpV4Client::new(retry_times, &exec);

        let resp = client.request(&final_url, &body, hyper::Method::POST, request.headers);
        return resp
    }
}

