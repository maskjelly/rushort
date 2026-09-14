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
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        stream.read_to_end(&mut response),
    )
    .await
    .unwrap()
    .unwrap();
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

#[tokio::test]
async fn drains_more_than_one_batch_without_more_input() {
    let addr = start_server().await;
    let mut request = format!("GET /health HTTP/1.1\r\nHost: {addr}\r\n\r\n").repeat(599);
    request.push_str(&get_request(addr, "/health"));
    let response =
        tokio::time::timeout(std::time::Duration::from_secs(2), roundtrip(addr, &request))
            .await
            .expect("pipeline stalled");
    assert_eq!(response.matches("HTTP/1.1 200 OK").count(), 600);
}

#[tokio::test]
async fn rejects_ambiguous_or_invalid_http() {
    let addr = start_server().await;
    let requests = [
        "GET /health WHAT\r\nHost: x\r\n\r\n",
        "GET /health HTTP/1.1\r\nConnection: close\r\n\r\n",
        "GET /health HTTP/1.1\r\nHost: x\r\nHost: y\r\n\r\n",
        "GET /health HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\nContent-Length: 0\r\n\r\n",
        "GET /health HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\nContent-Length: 0\r\n\r\n0\r\n\r\n",
        "GET /health HTTP/1.1\r\nHost: x\r\nContent-Length : 0\r\n\r\n",
        "GET /health HTTP/1.1\r\nHost: x\r\nContent-Length: +1\r\n\r\nx",
    ];
    for request in requests {
        let response =
            tokio::time::timeout(std::time::Duration::from_secs(2), roundtrip(addr, request))
                .await
                .unwrap();
        assert_eq!(status_of(&response), 400, "accepted {request:?}");
        assert!(response.contains("connection: close"));
    }
}

#[tokio::test]
async fn enforces_header_body_limits_and_expectations() {
    let addr = start_server().await;
    for (header, status) in [
        (format!("X-Pad: {}", "x".repeat(17000)), 431),
        ("Content-Length: 8193".into(), 413),
        ("Expect: unsupported".into(), 417),
    ] {
        let response = roundtrip(
            addr,
            &format!("GET /health HTTP/1.1\r\nHost: x\r\n{header}\r\nConnection: close\r\n\r\n"),
        )
        .await;
        assert_eq!(status_of(&response), status);
    }
}

#[tokio::test]
async fn connection_close_token_and_http10_close() {
    let addr = start_server().await;
    for req in [
        "GET /health HTTP/1.0\r\nHost: x\r\n\r\n",
        "GET /health HTTP/1.1\r\nHost: x\r\nConnection: keep-alive, close\r\n\r\n",
    ] {
        let response =
            tokio::time::timeout(std::time::Duration::from_secs(2), roundtrip(addr, req))
                .await
                .unwrap();
        assert_eq!(status_of(&response), 200);
        assert!(response.contains("connection: close"));
    }
}

#[tokio::test]
async fn fragmented_expect_body_gets_one_continue() {
    use tokio::time::{Duration, timeout};
    let addr = start_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    let body = "{\"url\":\"https://example.com/fragment\"}";
    let header = format!(
        "POST /api/shorten HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n",
        body.len()
    );
    s.write_all(header.as_bytes()).await.unwrap();
    let mut interim = [0; 25];
    timeout(Duration::from_secs(1), s.read_exact(&mut interim))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&interim, b"HTTP/1.1 100 Continue\r\n\r\n");
    for chunk in body.as_bytes().chunks(3) {
        s.write_all(chunk).await.unwrap();
        tokio::task::yield_now().await;
    }
    let mut response = Vec::new();
    timeout(Duration::from_secs(1), s.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status_of(&String::from_utf8(response).unwrap()), 201);
}

#[tokio::test]
async fn protects_writes_and_uses_configured_public_origin() {
    use rushort::{ServerConfig, serve_with_shutdown};
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let token = "a".repeat(32);
    let cfg = ServerConfig {
        write_token: Some(token.clone()),
        public_base: "https://s.example".into(),
        ..Default::default()
    };
    tokio::spawn(serve_with_shutdown(
        l,
        Arc::new(Store::new()),
        cfg,
        std::future::pending::<()>(),
    ));
    let req = post_request(addr, "https://example.com/");
    assert_eq!(status_of(&roundtrip(addr, &req).await), 401);
    let req = req.replace(
        "Connection: close",
        &format!("Authorization: Bearer {token}\r\nConnection: close"),
    );
    let response = roundtrip(addr, &req).await;
    assert_eq!(status_of(&response), 201);
    assert!(response.contains("https://s.example/0"));
    let stats = roundtrip(addr, &get_request(addr, "/api/stats")).await;
    assert_eq!(status_of(&stats), 401);
}

#[tokio::test]
async fn partial_header_deadline_is_absolute() {
    use rushort::{ServerConfig, serve_with_shutdown};
    use tokio::time::{Duration, sleep, timeout};
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let cfg = ServerConfig {
        io_timeout: Duration::from_millis(100),
        ..Default::default()
    };
    tokio::spawn(serve_with_shutdown(
        l,
        Arc::new(Store::new()),
        cfg,
        std::future::pending::<()>(),
    ));
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(b"GET /").await.unwrap();
    sleep(Duration::from_millis(70)).await;
    s.write_all(b"health").await.unwrap();
    sleep(Duration::from_millis(60)).await;
    let mut buf = [0; 1];
    let n = timeout(Duration::from_millis(100), s.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 0);
}

#[test]
fn durable_store_survives_reopen_and_rejects_second_owner() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let store = Store::open(&path, 8, 10).unwrap();
    let code = store.shorten("https://example.com/a").unwrap();
    assert!(Store::open(&path, 8, 10).is_err());
    drop(store);
    let store = Store::open(&path, 16, 10).unwrap();
    assert_eq!(
        store.resolve(&code).as_deref(),
        Some("https://example.com/a")
    );
    assert_ne!(store.shorten("https://example.com/b").unwrap(), code);
}

#[test]
fn capacity_and_url_validity_are_enforced_at_storage_boundary() {
    let store = Store::memory(1, 1);
    for url in [
        "http://",
        "https://?x",
        "https://user:pass@example.com",
        "https://example.com\\evil",
    ] {
        assert!(store.shorten(url).is_err());
    }
    store.shorten("https://example.com").unwrap();
    assert_eq!(
        store.shorten("https://example.com/2").unwrap_err().kind(),
        std::io::ErrorKind::StorageFull
    );
    assert_eq!(store.len(), 1);
    assert!(store.resolve("00").is_none());
}

#[tokio::test]
async fn head_has_get_headers_without_a_body() {
    let addr = start_server().await;
    for path in ["/health", "/missing-code"] {
        let req = get_request(addr, path).replacen("GET ", "HEAD ", 1);
        let response = roundtrip(addr, &req).await;
        assert!(response.ends_with("\r\n\r\n"));
        assert_eq!(
            status_of(&response),
            if path == "/health" { 200 } else { 404 }
        );
    }
}

#[tokio::test]
async fn connection_limit_releases_after_disconnect() {
    use rushort::{ServerConfig, serve_with_shutdown};
    use tokio::time::{Duration, sleep, timeout};
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let cfg = ServerConfig {
        max_connections: 1,
        ..Default::default()
    };
    tokio::spawn(serve_with_shutdown(
        l,
        Arc::new(Store::new()),
        cfg,
        std::future::pending::<()>(),
    ));
    let mut first = TcpStream::connect(addr).await.unwrap();
    first
        .write_all(b"GET /health HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .unwrap();
    let mut buf = [0; 256];
    assert!(first.read(&mut buf).await.unwrap() > 0);
    let mut second = TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        timeout(Duration::from_secs(1), second.read(&mut buf))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    drop(first);
    sleep(Duration::from_millis(20)).await;
    assert_eq!(
        status_of(&roundtrip(addr, &get_request(addr, "/health")).await),
        200
    );
}

#[tokio::test]
async fn metrics_counts_requests_redirects_and_errors() {
    let addr = start_server().await;
    roundtrip(addr, &get_request(addr, "/health")).await;
    let response = roundtrip(addr, &post_request(addr, "https://example.com/m")).await;
    let code = code_of(&response);
    roundtrip(addr, &get_request(addr, &format!("/{code}"))).await;
    roundtrip(addr, &get_request(addr, "/no-such-code")).await;
    let body = roundtrip(addr, &get_request(addr, "/api/metrics")).await;
    assert!(body.contains("\"requests\":4"), "got: {body}");
    assert!(body.contains("\"redirects\":1"), "got: {body}");
    assert!(body.contains("\"writes\":1"), "got: {body}");
    assert!(body.contains("\"errors_4xx\":1"), "got: {body}");
    assert!(body.contains("\"urls\":1"), "got: {body}");
    assert!(body.contains("access-control-allow-origin: *"), "got: {body}");
}
