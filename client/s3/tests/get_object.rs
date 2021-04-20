use common::runtime::Executor;
use s3::s3_client::S3Client;

#[test]
fn test_get_object()->Result<(), String> {
    let region = String::from("cn-bj-1");
    let endpoint = String::from("s3.test.com:8080");
    let ak = String::from("hehehehe");
    let sk = String::from("hehehehe");
    let target_bucket = String::from("test-bucket");
    let target_object = String::from("test-object-2");
    let offset: u64 = 0;
    let size: u32 = 5;
    let exec = Executor::create();

    let s3_client = S3Client::new(&region, &endpoint, &ak, &sk);
    let resp = exec.get_runtime().block_on(s3_client.get_object(&target_bucket, &target_object, &offset, &size));
    match resp {
        Ok(result) => {
            if result.is_empty() {
                return Err(format!("get object got empty body"));
            }

            let obj = String::from_utf8(result);
            match obj {
                Ok(bstr) => {
                    println!("get object resp is: {}", bstr);
                    return Ok(());
                }
                Err(error) => {
                    return Err(format!("get object got invalid body with error: {}", error));
                }
            }
        }
        Err(error) => {
            return Err(format!("Failed to get object, error {:?}", error));
        }
    }
}
