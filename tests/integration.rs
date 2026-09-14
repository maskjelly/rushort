use std::net::SocketAddr;
use std::sync::Arc;

use rushort::{Store, serve};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

async fn start_server() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve(listener, Arc::new(Store::new())));
    addr
}

async fn roundtrip(addr: SocketAddr, request: &str) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    String::from_utf8_lossy(&response).into_owned()
}

fn post_request(addr: SocketAddr, url: &str) -> String {
    let body = format!("{{\"url\":\"{url}\"}}");
    format!(
        "POST /api/shorten HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

fn get_request(addr: SocketAddr, path: &str) -> String {
    format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n")
}

fn status_of(response: &str) -> u16 {
    response
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or(0)
}

fn code_of(response: &str) -> String {
    response
        .split("\"code\":\"")
        .nth(1)
        .and_then(|rest| rest.split('"').next())
        .expect("code in response")
        .to_owned()
}

#[tokio::test]
async fn health_over_tcp() {
    let addr = start_server().await;
    let response = roundtrip(addr, &get_request(addr, "/health")).await;
    assert_eq!(status_of(&response), 200);
    assert!(response.ends_with("ok"), "got: {response}");
}

#[tokio::test]
async fn shorten_then_redirect_roundtrip() {
    let addr = start_server().await;
    let response = roundtrip(addr, &post_request(addr, "https://example.com/hello?x=1")).await;
    assert_eq!(status_of(&response), 201, "got: {response}");
    assert!(response.contains("\"long_url\":\"https://example.com/hello?x=1\""));
    let code = code_of(&response);

    let response = roundtrip(addr, &get_request(addr, &format!("/{code}"))).await;
    assert_eq!(status_of(&response), 302, "got: {response}");
    assert!(
        response.contains("location: https://example.com/hello?x=1"),
        "got: {response}"
    );
}

#[tokio::test]
async fn invalid_urls_rejected() {
    let addr = start_server().await;
    for bad in [
        "ftp://example.com",
        "javascript:alert(1)",
        "",
        "not a url",
        "https://exam ple.com",
        "https://exämple.com",
    ] {
        let response = roundtrip(addr, &post_request(addr, bad)).await;
        assert_eq!(status_of(&response), 400, "should reject `{bad}`");
    }
}

#[tokio::test]
async fn unknown_code_is_404() {
    let addr = start_server().await;
    let response = roundtrip(addr, &get_request(addr, "/does-not-exist")).await;
    assert_eq!(status_of(&response), 404);
}

#[tokio::test]
async fn stats_counts_urls() {
    let addr = start_server().await;
    for i in 0..7 {
        let response = roundtrip(
            addr,
            &post_request(addr, &format!("https://example.com/{i}")),
        )
        .await;
        assert_eq!(status_of(&response), 201);
    }
    let response = roundtrip(addr, &get_request(addr, "/api/stats")).await;
    assert_eq!(status_of(&response), 200);
    assert!(response.contains("\"urls\":7"), "got: {response}");
}

#[tokio::test]
async fn keep_alive_pipelined_requests() {
    let addr = start_server().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let first = format!("GET /health HTTP/1.1\r\nHost: {addr}\r\nConnection: keep-alive\r\n\r\n");
    let second = post_request(addr, "https://example.com/keep")
        .replace("Connection: close", "Connection: keep-alive");
    stream.write_all(first.as_bytes()).await.unwrap();
    stream.write_all(second.as_bytes()).await.unwrap();

    let mut buf = vec![0u8; 4096];
    let mut got = Vec::new();
    while !got.windows(14).any(|w| w == b"HTTP/1.1 201 C") {
        let n = stream.read(&mut buf).await.unwrap();
        assert!(n > 0, "connection closed early");
        got.extend_from_slice(&buf[..n]);
    }
    let text = String::from_utf8_lossy(&got);
    assert!(text.contains("HTTP/1.1 200 OK"), "got: {text}");
}

#[tokio::test]
async fn concurrent_clients_verify_all_redirects() {
    let addr = start_server().await;
    let mut handles = Vec::new();
    for task in 0..24u32 {
        handles.push(tokio::spawn(async move {
            for i in 0..20u32 {
                let url = format!("https://example.com/{task}/{i}");
                let response = roundtrip(addr, &post_request(addr, &url)).await;
                assert_eq!(status_of(&response), 201);
                let code = code_of(&response);
                let response = roundtrip(addr, &get_request(addr, &format!("/{code}"))).await;
                assert_eq!(status_of(&response), 302);
                assert!(
                    response.contains(&format!("location: {url}")),
                    "wrong redirect for {code}: {response}"
                );
            }
        }));
    }
    for handle in handles {
        handle.await.unwrap();
    }
}
