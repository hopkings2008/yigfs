use common::runtime::Executor;
use s3::s3_client::S3Client;

#[test]
fn test_head_object()->Result<(), String> {
    let region = String::from("cn-bj-1");
    let endpoint = String::from("s3.test.com:8080");
    let ak = String::from("hehehehe");
    let sk = String::from("hehehehe");
    let target_bucket = String::from("test-bucket");
    let target_object = String::from("test-object-1");
    let exec = Executor::create();

    let s3_client = S3Client::new(&region, &endpoint, &ak, &sk);
    let resp = exec.get_runtime().block_on(s3_client.head_object(&target_bucket, &target_object));
    match resp {
        Ok(result) => {
            println!("resp is {:?}", result);
            return Ok(());
        }
        Err(error) => {
            return Err(format!("Failed to head object, error {:?}", error));
        }
    }
}
