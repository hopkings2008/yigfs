use common::http_client;
use common::runtime::Executor;
use log::info;

#[test]
fn test_http_client_get()->Result<(), String> {
    let url = String::from("http://www.baidu.com");
    let retry_times = 3;
    let exec = Executor::create();
    let client = http_client::HttpClient::new(retry_times);
    let resp = exec.get_runtime().block_on(client.request(&url, &String::from("hello world").as_bytes(), &http_client::HttpMethod::Get, false))?;
    if resp.status >= 300 {
        return Err(format!("got invalid status {}", resp.status));
    }
    if resp.body.is_empty() {
        return Err(format!("got empty body"));
    }
    info!("{}", resp.body);
    return Ok(());
}

#[test]
fn test_https_client_get()->Result<(), String> {
    let url = String::from("https://www.baidu.com");
    let retry_times = 3;
    let exec = Executor::create();
    let client = http_client::HttpClient::new(retry_times);
    let resp = exec.get_runtime().block_on(client.request(&url, &String::from("hello world").as_bytes(), &http_client::HttpMethod::Get, false))?;
    if resp.status >= 300 {
        return Err(format!("got invalid status {}", resp.status));
    }
    if resp.body.is_empty() {
        return Err(format!("got empty body"));
    }
    info!("{}", resp.body);
    return Ok(());
}
