use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    task::JoinSet,
    time::{Instant, timeout, timeout_at},
};
mod store;
pub use store::Store;

const ALPHABET: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
const MAX_URL_LEN: usize = 2048;
const MAX_HEAD: usize = 16 * 1024;
const MAX_BODY: usize = 8 * 1024;
const MAX_BATCH: usize = 256;

#[inline]
pub fn base62_encode(mut n: u64) -> String {
    if n == 0 {
        return "0".to_owned();
    }
    let mut buf = [0u8; 11];
    let mut i = buf.len();
    while n > 0 {
        i -= 1;
        buf[i] = ALPHABET[(n % 62) as usize];
        n /= 62;
    }
    String::from_utf8(buf[i..].to_vec()).expect("base62 alphabet is ASCII")
}

pub fn base62_decode(s: &str) -> Option<u64> {
    if s.is_empty() {
        return None;
    }
    let mut n: u64 = 0;
    for b in s.bytes() {
        let d = match b {
            b'0'..=b'9' => b - b'0',
            b'A'..=b'Z' => b - b'A' + 10,
            b'a'..=b'z' => b - b'a' + 36,
            _ => return None,
        };
        n = n.checked_mul(62)?.checked_add(d as u64)?;
    }
    Some(n)
}

#[derive(Clone)]
pub struct ServerConfig {
    pub public_base: String,
    pub write_token: Option<String>,
    pub max_connections: usize,
    pub io_timeout: Duration,
    pub max_writes: usize,
    pub metrics: Arc<Metrics>,
}
impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            public_base: "http://127.0.0.1:8080".into(),
            write_token: None,
            max_connections: 1024,
            io_timeout: Duration::from_secs(5),
            max_writes: 32,
            metrics: Arc::new(Metrics::new()),
        }
    }
}

/// Lock-free cumulative counters. The dashboard derives RPS/QPS by sampling.
pub struct Metrics {
    requests: AtomicU64,
    redirects: AtomicU64,
    writes: AtomicU64,
    errors_4xx: AtomicU64,
    errors_5xx: AtomicU64,
    started: std::time::Instant,
    started_unix: u64,
}
impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
impl Metrics {
    pub fn new() -> Self {
        Self {
            requests: AtomicU64::new(0),
            redirects: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            errors_4xx: AtomicU64::new(0),
            errors_5xx: AtomicU64::new(0),
            started: std::time::Instant::now(),
            started_unix: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        }
    }
    fn record(&self, status: u16) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        match status {
            302 => {
                self.redirects.fetch_add(1, Ordering::Relaxed);
            }
            201 => {
                self.writes.fetch_add(1, Ordering::Relaxed);
            }
            400..=499 => {
                self.errors_4xx.fetch_add(1, Ordering::Relaxed);
            }
            500..=599 => {
                self.errors_5xx.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }
    pub fn snapshot(&self, store: &Store) -> String {
        let load = |a: &AtomicU64| a.load(Ordering::Relaxed);
        format!(
            "{{\"uptime_s\":{},\"started_unix\":{},\"requests\":{},\"redirects\":{},\"writes\":{},\"errors_4xx\":{},\"errors_5xx\":{},\"urls\":{},\"capacity\":{}}}",
            self.started.elapsed().as_secs(),
            self.started_unix,
            load(&self.requests),
            load(&self.redirects),
            load(&self.writes),
            load(&self.errors_4xx),
            load(&self.errors_5xx),
            store.len(),
            store.capacity(),
        )
    }
}

pub fn validate_url(url: &str) -> Result<(), &'static str> {
    if url.len() > MAX_URL_LEN || !url.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
        return Err("url must contain 1..2048 printable ASCII bytes");
    }
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err("url must start with http:// or https://");
    }
    let parsed = url::Url::parse(url).map_err(|_| "invalid URL")?;
    if parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || url.contains('\\')
    {
        return Err("url requires a host and must not contain credentials or backslashes");
    }
    let authority = url
        .split_once("://")
        .unwrap()
        .1
        .split(['/', '?', '#'])
        .next()
        .unwrap();
    if authority.is_empty() {
        return Err("url requires a host");
    }
    Ok(())
}

struct Parsed<'a> {
    method: &'a str,
    path: &'a str,
    content_length: usize,
    close: bool,
    expect: bool,
    auth: &'a [u8],
}
fn parse_head(head: &[u8]) -> Result<Parsed<'_>, u16> {
    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut req = httparse::Request::new(&mut headers);
    if !req.parse(head).map_err(|_| 400u16)?.is_complete() {
        return Err(400);
    }
    let mut length = None;
    let mut host = None;
    let mut close = false;
    let mut keep = false;
    let mut expect = false;
    let mut auth = None;
    for h in req.headers.iter() {
        let v = h.value.trim_ascii();
        if h.name.eq_ignore_ascii_case("content-length") {
            // Reject duplicate fields, including identical ones: no ambiguous framing.
            if length.is_some() || v.is_empty() || !v.iter().all(u8::is_ascii_digit) {
                return Err(400);
            }
            length = Some(
                std::str::from_utf8(v)
                    .map_err(|_| 400u16)?
                    .parse::<usize>()
                    .map_err(|_| 400u16)?,
            );
        } else if h.name.eq_ignore_ascii_case("transfer-encoding") {
            return Err(400);
        } else if h.name.eq_ignore_ascii_case("host") {
            if host.is_some()
                || v.is_empty()
                || !v
                    .iter()
                    .all(|b| b.is_ascii_alphanumeric() || b".-:[]".contains(b))
            {
                return Err(400);
            }
            std::str::from_utf8(v)
                .map_err(|_| 400u16)?
                .parse::<http::uri::Authority>()
                .map_err(|_| 400u16)?;
            host = Some(v);
        } else if h.name.eq_ignore_ascii_case("connection") {
            for token in v.split(|b| *b == b',') {
                close |= token.trim_ascii().eq_ignore_ascii_case(b"close");
                keep |= token.trim_ascii().eq_ignore_ascii_case(b"keep-alive");
            }
        } else if h.name.eq_ignore_ascii_case("expect") {
            if expect || !v.eq_ignore_ascii_case(b"100-continue") {
                return Err(417);
            }
            expect = true;
        } else if h.name.eq_ignore_ascii_case("authorization") {
            if auth.is_some() {
                return Err(400);
            }
            auth = Some(v);
        }
    }
    if req.version == Some(1) && host.is_none() {
        return Err(400);
    }
    let path = req.path.ok_or(400u16)?;
    if !path.starts_with('/') || path.contains('#') {
        return Err(400);
    }
    let content_length = length.unwrap_or(0);
    if content_length > MAX_BODY {
        return Err(413);
    }
    Ok(Parsed {
        method: req.method.ok_or(400u16)?,
        path,
        content_length,
        close: close || (req.version == Some(0) && !keep),
        expect,
        auth: auth.unwrap_or(b""),
    })
}
fn connection(out: &mut Vec<u8>, close: bool) {
    out.extend_from_slice(if close {
        b"connection: close\r\n\r\n"
    } else {
        b"connection: keep-alive\r\n\r\n"
    });
}
fn json(out: &mut Vec<u8>, status: u16, body: &str, close: bool) {
    use std::io::Write;
    let reason = match status {
        200 => "OK",
        201 => "Created",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        405 => "Method Not Allowed",
        413 => "Content Too Large",
        417 => "Expectation Failed",
        431 => "Request Header Fields Too Large",
        503 => "Service Unavailable",
        _ => "Internal Server Error",
    };
    write!(out,"HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\ncache-control: no-store\r\n", body.len()).unwrap();
    if status == 401 {
        out.extend_from_slice(b"www-authenticate: Bearer\r\n");
    }
    if status == 405 {
        out.extend_from_slice(b"allow: GET, HEAD, POST\r\n");
    }
    connection(out, close);
    out.extend_from_slice(body.as_bytes());
}
fn error(out: &mut Vec<u8>, status: u16, close: bool) {
    json(out, status, &format!("{{\"error\":{status}}}"), close);
}
fn authorized(auth: &[u8], token: Option<&str>) -> bool {
    let Some(token) = token else {
        return true;
    };
    let Some(value) = auth.strip_prefix(b"Bearer ") else {
        return false;
    };
    // Fixed work for equal-length secrets. Keys are restricted to long random ASCII tokens.
    use subtle::ConstantTimeEq;
    bool::from(value.ct_eq(token.as_bytes()))
}
async fn route(
    out: &mut Vec<u8>,
    store: &Arc<Store>,
    config: &ServerConfig,
    writes: &Arc<Semaphore>,
    head: &Parsed<'_>,
    body: &[u8],
) -> u16 {
    use std::io::Write;
    let path = head.path.split('?').next().unwrap();
    let close = head.close;
    let method = if head.method == "HEAD" {
        "GET"
    } else {
        head.method
    };
    match (method, path) {
        ("GET", "/health" | "/ready") => {
            out.extend_from_slice(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n");
            connection(out, close);
            out.extend_from_slice(b"ok");
            200
        }
        ("GET", "/api/metrics") => {
            // Public and CORS-open: powers live dashboards. Counts only, no URLs.
            let snapshot = config.metrics.snapshot(store);
            write!(out, "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\ncache-control: no-store\r\naccess-control-allow-origin: *\r\n", snapshot.len()).unwrap();
            connection(out, close);
            out.extend_from_slice(snapshot.as_bytes());
            200
        }
        ("GET", "/api/stats") => {
            if !authorized(head.auth, config.write_token.as_deref()) {
                error(out, 401, close);
                return 401;
            }
            json(
                out,
                200,
                &format!(
                    "{{\"urls\":{},\"shards\":{},\"capacity\":{}}}",
                    store.len(),
                    store.shard_count(),
                    store.capacity()
                ),
                close,
            );
            200
        }
        ("POST", "/api/shorten") => {
            if !authorized(head.auth, config.write_token.as_deref()) {
                error(out, 401, close);
                return 401;
            }
            let Ok(value) = serde_json::from_slice::<serde_json::Value>(body) else {
                error(out, 400, close);
                return 400;
            };
            let Some(url) = value.get("url").and_then(|v| v.as_str()) else {
                error(out, 400, close);
                return 400;
            };
            if validate_url(url).is_err() {
                error(out, 400, close);
                return 400;
            }
            let Ok(permit) = writes.clone().try_acquire_owned() else {
                error(out, 503, close);
                return 503;
            };
            let owned = url.to_owned();
            let store = store.clone();
            let result = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                store.shorten(&owned)
            })
            .await;
            match result {
                Ok(Ok(code)) => {
                    json(out,201,&serde_json::json!({"code":code,"short_url":format!("{}/{code}",config.public_base),"long_url":url}).to_string(),close);
                    201
                }
                Ok(Err(e)) if e.kind() == io::ErrorKind::StorageFull => {
                    error(out, 503, close);
                    503
                }
                _ => {
                    eprintln!("rushort: storage write failed");
                    error(out, 503, close);
                    503
                }
            }
        }
        ("GET", _) => match path.strip_prefix('/').and_then(|code| store.resolve(code)) {
            Some(url) => {
                out.extend_from_slice(b"HTTP/1.1 302 Found\r\nlocation: ");
                out.extend_from_slice(url.as_bytes());
                out.extend_from_slice(b"\r\ncontent-length: 0\r\n");
                connection(out, close);
                302
            }
            None => {
                error(out, 404, close);
                404
            }
        },
        ("POST", _) => {
            error(out, 404, close);
            404
        }
        _ => {
            error(out, 405, close);
            405
        }
    }
}
fn find_head(buf: &[u8]) -> Option<usize> {
    memchr::memmem::find(buf, b"\r\n\r\n").map(|n| n + 4)
}
async fn send(stream: &mut TcpStream, bytes: &[u8], limit: Duration) -> io::Result<()> {
    timeout(limit, stream.write_all(bytes)).await?
}
async fn handle_conn(
    mut stream: TcpStream,
    store: Arc<Store>,
    cfg: Arc<ServerConfig>,
    writes: Arc<Semaphore>,
) -> io::Result<()> {
    let mut buf = Vec::with_capacity(4096);
    let mut tmp = [0u8; 16 * 1024];
    let mut out = Vec::with_capacity(4096);
    let mut deadline = Instant::now() + cfg.io_timeout;
    let mut continued = false;
    loop {
        let mut start = 0;
        let mut processed = 0;
        let mut close = false;
        out.clear();
        while processed < MAX_BATCH {
            let remaining = &buf[start..];
            let Some(head_len) = find_head(remaining) else {
                if remaining.len() >= MAX_HEAD {
                    error(&mut out, 431, true);
                    cfg.metrics.record(431);
                    close = true;
                }
                break;
            };
            if head_len > MAX_HEAD {
                error(&mut out, 431, true);
                cfg.metrics.record(431);
                close = true;
                break;
            }
            let head = match parse_head(&remaining[..head_len]) {
                Ok(h) => h,
                Err(status) => {
                    error(&mut out, status, true);
                    cfg.metrics.record(status);
                    close = true;
                    break;
                }
            };
            let total = head_len + head.content_length;
            if remaining.len() < total {
                if head.expect && !continued {
                    send(&mut stream, &out, cfg.io_timeout).await?;
                    out.clear();
                    send(
                        &mut stream,
                        b"HTTP/1.1 100 Continue\r\n\r\n",
                        cfg.io_timeout,
                    )
                    .await?;
                    continued = true;
                }
                break;
            }
            let response_start = out.len();
            let status = route(
                &mut out,
                &store,
                &cfg,
                &writes,
                &head,
                &remaining[head_len..total],
            )
            .await;
            cfg.metrics.record(status);
            if head.method == "HEAD" {
                let header_len =
                    find_head(&out[response_start..]).expect("complete response header");
                out.truncate(response_start + header_len);
            }
            close = head.close;
            start += total;
            processed += 1;
            continued = false;
            if close {
                break;
            }
        }
        if !out.is_empty() {
            send(&mut stream, &out, cfg.io_timeout).await?;
        }
        if close {
            // Send FIN before bounded draining, so unread rejected input does not reset away the response.
            timeout(cfg.io_timeout, stream.shutdown()).await??;
            let _ = timeout(Duration::from_millis(100), async {
                for _ in 0..4 {
                    if stream.read(&mut tmp).await.unwrap_or(0) == 0 {
                        break;
                    }
                }
            })
            .await;
            return Ok(());
        }
        if start > 0 {
            buf.drain(..start);
            deadline = Instant::now() + cfg.io_timeout;
        }
        if processed == MAX_BATCH {
            tokio::task::yield_now().await;
            continue;
        }
        let n = timeout_at(deadline, stream.read(&mut tmp)).await??;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&tmp[..n]);
    }
}
/// Compatibility entry point for tests and embedded ephemeral use.
pub async fn serve(listener: TcpListener, store: Arc<Store>) -> io::Result<()> {
    serve_with_shutdown(
        listener,
        store,
        ServerConfig::default(),
        std::future::pending::<()>(),
    )
    .await
}
pub async fn serve_with_shutdown(
    listener: TcpListener,
    store: Arc<Store>,
    config: ServerConfig,
    shutdown: impl std::future::Future<Output = ()>,
) -> io::Result<()> {
    let slots = Arc::new(Semaphore::new(config.max_connections));
    let writes = Arc::new(Semaphore::new(config.max_writes));
    let config = Arc::new(config);
    let mut tasks = JoinSet::new();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            biased;
            _=&mut shutdown => break,
            Some(result)=tasks.join_next(), if !tasks.is_empty() => { if result.is_err() { eprintln!("rushort: connection task failed"); } }
            accepted=listener.accept() => {
                let (stream,_)=accepted?;
                let Ok(permit)=slots.clone().try_acquire_owned() else { drop(stream);continue; };
                stream.set_nodelay(true)?; let store=store.clone();let config=config.clone();let writes=writes.clone();
                tasks.spawn(async move {let _permit=permit; let _=handle_conn(stream,store,config,writes).await;});
            }
        }
    }
    drop(listener);
    if timeout(Duration::from_secs(10), async {
        while tasks.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        tasks.abort_all();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base62_roundtrip() {
        assert_eq!(base62_encode(0), "0");
        assert_eq!(base62_encode(1), "1");
        assert_eq!(base62_encode(61), "z");
        assert_eq!(base62_encode(62), "10");
        assert_eq!(base62_encode(62 * 62 - 1), "zz");
        for n in [0u64, 1, 61, 62, 3843, 3844, u64::MAX / 2, u64::MAX - 1] {
            let encoded = base62_encode(n);
            assert_eq!(base62_decode(&encoded), Some(n), "roundtrip failed for {n}");
        }
        assert_eq!(base62_decode(""), None);
        assert_eq!(base62_decode("a!"), None);
    }

    #[test]
    fn codes_are_unique_across_threads() {
        let store = Arc::new(Store::with_shards(16));
        let mut handles = Vec::new();
        for t in 0..8u64 {
            let store = store.clone();
            handles.push(std::thread::spawn(move || {
                let mut codes = Vec::with_capacity(5_000);
                for i in 0..5_000u64 {
                    codes.push(
                        store
                            .shorten(&format!("https://example.com/{t}/{i}"))
                            .unwrap(),
                    );
                }
                codes
            }));
        }
        let mut all = Vec::new();
        for h in handles {
            all.extend(h.join().unwrap());
        }
        assert_eq!(store.len(), 40_000);
        for code in &all {
            assert!(store.resolve(code).is_some(), "missing code {code}");
        }
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), 40_000, "duplicate codes generated");
        assert!(store.resolve("definitely-not-a-code").is_none());
    }

    #[test]
    fn resolve_returns_right_url_per_shard() {
        let store = Store::with_shards(8);
        let mut codes = Vec::new();
        for i in 0..1_000 {
            codes.push((
                store.shorten(&format!("https://example.com/{i}")).unwrap(),
                format!("https://example.com/{i}"),
            ));
        }
        for (code, url) in &codes {
            assert_eq!(store.resolve(code).as_deref(), Some(url.as_str()));
        }
    }

    #[test]
    fn url_validation() {
        assert!(validate_url("https://example.com/a?b=c").is_ok());
        assert!(validate_url("http://example.com").is_ok());
        assert!(validate_url("ftp://example.com").is_err());
        assert!(validate_url("javascript:alert(1)").is_err());
        assert!(validate_url("").is_err());
        assert!(validate_url("https://exa mple.com").is_err());
        assert!(validate_url("https://exämple.com").is_err());
        assert!(validate_url(&format!("https://example.com/{}", "a".repeat(2100))).is_err());
    }
}
