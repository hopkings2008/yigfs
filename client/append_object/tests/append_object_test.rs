#[test]
fn test_appned_object()->Result<(), String> {
    let region = String::from("cn-bj-1");
    let host = String::from("s3.test.com:8080");
    let bucket = String::from("test-bucket");
    let object = String::from("test-object");
    let ak = String::from("hehehehe");
    let sk = String::from("hehehehe");
    let object_path = String::from("/home/test_object");
    let append_position: u128 = 0;

    let s3_client = append_object::S3Client::new(&region, &host, &bucket, &object, &ak, &sk, &object_path, append_position);
    let resp = s3_client.append_object()?;
    if resp.status >= 300 {
        return Err(format!("Failed to append object, got invalid status {}", resp.status));
    }

    println!("resp body {}, resp headers {:?}", resp.body, resp.headers);
    return Ok(());
}
