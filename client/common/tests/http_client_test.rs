use common::http_client;
#[test]
fn test_http_client_get()->Result<(), String> {
    let url = String::from("http://www.baidu.com");
    let retry_times = 3;
    let client = http_client::HttpClient::new(retry_times);
    let resp = client.request(&url, &String::from("hello world"), &http_client::HttpMethod::Get)?;
    if resp.status >= 300 {
        return Err(format!("got invalid status {}", resp.status));
    }
    if resp.body.is_empty() {
        return Err(format!("got empty body"));
    }
    println!("{}", resp.body);
    return Ok(());
}