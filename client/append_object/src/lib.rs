pub mod signature;

use std::io::prelude::*;
use std::fs::File;
use std::io::BufReader;
use signature::AwsCredentials;
use signature::SignedRequest;

use common::http_client::RespText;
use common::http_client::HttpClient;
use common::http_client::HttpMethod;

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

    pub async fn append_object_by_path(&self, object_path: &str, bucket: &str, object: &str, append_position: &u128) -> Result<RespText, String> {
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

        // send append object req
        let retry_times = 3;
        let mut client = HttpClient::new(retry_times);
        client.set_headers(request.headers);
        
        let resp = client.request(&final_url, &body, &HttpMethod::Post, true).await;
        return resp
    }

    pub async fn append_object(&self, data: &[u8], bucket: &str, object: &str, append_position: &u128) -> Result<RespText, String> {
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

        // send append object req
        let retry_times = 3;
        let mut client = HttpClient::new(retry_times);
        client.set_headers(request.headers);

        let resp = client.request(&final_url, data, &HttpMethod::Post, true).await;
        return resp
    }
}
