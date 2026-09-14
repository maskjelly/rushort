use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const ALPHABET: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
const MAX_URL_LEN: usize = 2048;
const MAX_HEAD: usize = 16 * 1024;
const MAX_BODY: usize = 8 * 1024;
const MAX_CODE_LEN: usize = 11;
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

struct Shard {
    urls: RwLock<Vec<Arc<str>>>,
}

pub struct Store {
    shards: Box<[Shard]>,
    next_write: AtomicU64,
}

impl Store {
    pub fn new() -> Self {
        Self::with_shards(64)
    }

    pub fn with_shards(n: usize) -> Self {
        assert!(n.is_power_of_two(), "shard count must be a power of two");
        let shards = (0..n)
            .map(|_| Shard {
                urls: RwLock::new(Vec::with_capacity(1024)),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            shards,
            next_write: AtomicU64::new(0),
        }
    }

    pub fn shorten(&self, url: &str) -> String {
        let ticket = self.next_write.fetch_add(1, Ordering::Relaxed);
        let shard_idx = (ticket as usize) & (self.shards.len() - 1);
        let shard = &self.shards[shard_idx];
        let mut urls = shard.urls.write();
        let index = urls.len() as u64;
        let id = shard_idx as u64 + index * self.shards.len() as u64;
        urls.push(Arc::from(url));
        base62_encode(id)
    }

    pub fn resolve(&self, code: &str) -> Option<Arc<str>> {
        let id = base62_decode(code)?;
        let shard_idx = (id % self.shards.len() as u64) as usize;
        let index = (id / self.shards.len() as u64) as usize;
        self.shards[shard_idx].urls.read().get(index).cloned()
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.urls.read().len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

pub fn validate_url(url: &str) -> Result<(), &'static str> {
    if url.is_empty() {
        return Err("url is empty");
    }
    if url.len() > MAX_URL_LEN {
        return Err("url exceeds 2048 characters");
    }
    if !url.starts_with("http://") && !url.starts_with("https://") {
        return Err("url must start with http:// or https://");
    }
    if !url.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
        return Err("url contains invalid characters");
    }
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Method {
    Get,
    Post,
    Other,
}

struct Parsed {
    method: Method,
    path: (usize, usize),
    host: (usize, usize),
    content_length: usize,
    close: bool,
    expect_continue: bool,
}

fn memchr(byte: u8, haystack: &[u8]) -> Option<usize> {
    haystack.iter().position(|&b| b == byte)
}

fn parse_usize(bytes: &[u8]) -> Option<usize> {
    if bytes.is_empty() {
        return None;
    }
    let mut n: usize = 0;
    for &b in bytes {
        if !b.is_ascii_digit() {
            return None;
        }
        n = n.checked_mul(10)?.checked_add((b - b'0') as usize)?;
    }
    Some(n)
}

fn parse_head(head: &[u8]) -> Option<Parsed> {
    let line_end = find_subslice(head, b"\r\n")?;
    let request_line = &head[..line_end];
    let sp1 = memchr(b' ', request_line)?;
    let method = match &request_line[..sp1] {
        b"GET" => Method::Get,
        b"POST" => Method::Post,
        _ => Method::Other,
    };
    let rest = &request_line[sp1 + 1..];
    let sp2 = memchr(b' ', rest)?;
    let path = (sp1 + 1, sp1 + 1 + sp2);
    let mut close = &rest[sp2 + 1..] == b"HTTP/1.0";

    let mut content_length = 0usize;
    let mut host = (0, 0);
    let mut expect_continue = false;
    let mut pos = line_end + 2;
    while pos < head.len() {
        let end = match find_subslice(&head[pos..], b"\r\n") {
            Some(offset) => pos + offset,
            None => head.len(),
        };
        let line = &head[pos..end];
        if let Some(colon) = memchr(b':', line) {
            let name = &line[..colon];
            let mut vstart = pos + colon + 1;
            let vend = end;
            while vstart < vend && (head[vstart] == b' ' || head[vstart] == b'\t') {
                vstart += 1;
            }
            let mut vstop = vend;
            while vstop > vstart && (head[vstop - 1] == b' ' || head[vstop - 1] == b'\t') {
                vstop -= 1;
            }
            let value = &head[vstart..vstop];
            if name.eq_ignore_ascii_case(b"content-length") {
                content_length = parse_usize(value)?;
            } else if name.eq_ignore_ascii_case(b"connection") {
                if value.eq_ignore_ascii_case(b"close") {
                    close = true;
                } else if value.eq_ignore_ascii_case(b"keep-alive") {
                    close = false;
                }
            } else if name.eq_ignore_ascii_case(b"expect") {
                expect_continue = value.eq_ignore_ascii_case(b"100-continue");
            } else if name.eq_ignore_ascii_case(b"host") {
                host = (vstart, vstop);
            }
        }
        if end == head.len() {
            break;
        }
        pos = end + 2;
    }
    Some(Parsed {
        method,
        path,
        host,
        content_length,
        close,
        expect_continue,
    })
}

fn append_json_response(out: &mut Vec<u8>, status: u16, reason: &str, body: &str) {
    out.extend_from_slice(
        format!(
            "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: keep-alive\r\n\r\n",
            body.len()
        )
        .as_bytes(),
    );
    out.extend_from_slice(body.as_bytes());
}

fn append_json_error(out: &mut Vec<u8>, status: u16, message: &str) {
    append_json_response(
        out,
        status,
        if status == 404 {
            "Not Found"
        } else {
            "Bad Request"
        },
        &format!("{{\"error\":\"{message}\"}}"),
    );
}

fn append_not_found(out: &mut Vec<u8>) {
    append_json_error(out, 404, "short code not found");
}

fn append_redirect(out: &mut Vec<u8>, url: &str) {
    out.extend_from_slice(b"HTTP/1.1 302 Found\r\nlocation: ");
    out.extend_from_slice(url.as_bytes());
    out.extend_from_slice(b"\r\ncontent-length: 0\r\nconnection: keep-alive\r\n\r\n");
}

fn append_shorten_response(out: &mut Vec<u8>, store: &Store, host: &str, body: &[u8]) {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(body) else {
        return append_json_error(out, 400, "invalid json body");
    };
    let Some(url) = value.get("url").and_then(|v| v.as_str()) else {
        return append_json_error(out, 400, "missing `url` field");
    };
    let url = url.trim();
    if let Err(e) = validate_url(url) {
        return append_json_error(out, 400, e);
    }
    let code = store.shorten(url);
    let host = if host.is_empty() { "localhost" } else { host };
    let body = serde_json::json!({
        "code": code,
        "short_url": format!("http://{host}/{code}"),
        "long_url": url,
    })
    .to_string();
    append_json_response(out, 201, "Created", &body);
}

fn route(out: &mut Vec<u8>, store: &Store, method: Method, path: &str, host: &str, body: &[u8]) {
    match method {
        Method::Get if path == "/health" => {
            out.extend_from_slice(
                b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: keep-alive\r\n\r\nok",
            );
        }
        Method::Get if path == "/" => {
            let body = "rushort: POST /api/shorten {\"url\":\"...\"} | GET /{code} | GET /health | GET /api/stats";
            out.extend_from_slice(
                format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: {}\r\nconnection: keep-alive\r\n\r\n",
                    body.len()
                )
                .as_bytes(),
            );
            out.extend_from_slice(body.as_bytes());
        }
        Method::Get if path == "/api/stats" => {
            let body = format!(
                "{{\"urls\":{},\"shards\":{}}}",
                store.len(),
                store.shard_count()
            );
            append_json_response(out, 200, "OK", &body);
        }
        Method::Post if path == "/api/shorten" => append_shorten_response(out, store, host, body),
        Method::Get => {
            let Some(code) = path.strip_prefix('/') else {
                return append_not_found(out);
            };
            let code = code.split('?').next().unwrap_or(code);
            if code.is_empty() || code.len() > MAX_CODE_LEN {
                return append_not_found(out);
            }
            match store.resolve(code) {
                Some(url) => append_redirect(out, &url),
                None => append_not_found(out),
            }
        }
        _ => append_not_found(out),
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

async fn handle_conn(mut stream: TcpStream, store: Arc<Store>) -> io::Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(16 * 1024);
    let mut tmp = [0u8; 16 * 1024];
    let mut start = 0usize;
    let mut continue_sent: Option<usize> = None;
    let mut out: Vec<u8> = Vec::with_capacity(16 * 1024);

    loop {
        out.clear();
        let mut close = false;

        for _ in 0..MAX_BATCH {
            if start == buf.len() {
                break;
            }
            let Some(head_rel) = find_subslice(&buf[start..], b"\r\n\r\n") else {
                break;
            };
            let head_end = start + head_rel;
            let Some(head) = parse_head(&buf[start..head_end]) else {
                append_json_error(&mut out, 400, "bad request");
                start = buf.len();
                close = true;
                break;
            };
            if head.content_length > MAX_BODY {
                append_json_error(&mut out, 400, "request body too large");
                start = buf.len();
                close = true;
                break;
            }
            let total = head_end + 4 + head.content_length;
            if buf.len() < total {
                if head.expect_continue && continue_sent != Some(start) {
                    continue_sent = Some(start);
                    if !out.is_empty() {
                        stream.write_all(&out).await?;
                        out.clear();
                    }
                    stream.write_all(b"HTTP/1.1 100 Continue\r\n\r\n").await?;
                }
                break;
            }
            let path =
                std::str::from_utf8(&buf[start + head.path.0..start + head.path.1]).unwrap_or("");
            let host =
                std::str::from_utf8(&buf[start + head.host.0..start + head.host.1]).unwrap_or("");
            route(
                &mut out,
                &store,
                head.method,
                path,
                host,
                &buf[head_end + 4..total],
            );
            start = total;
            if head.close {
                close = true;
                break;
            }
        }

        if !out.is_empty() {
            stream.write_all(&out).await?;
        }
        if close {
            return Ok(());
        }

        if start > 0 {
            buf.drain(..start);
            start = 0;
            continue_sent = None;
        }
        if buf.len() > MAX_HEAD {
            return Ok(());
        }
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&tmp[..n]);
    }
}

pub async fn serve(listener: TcpListener, store: Arc<Store>) -> io::Result<()> {
    loop {
        let (stream, _addr) = listener.accept().await?;
        let _ = stream.set_nodelay(true);
        let store = store.clone();
        tokio::spawn(async move {
            let _ = handle_conn(stream, store).await;
        });
    }
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
                    codes.push(store.shorten(&format!("https://example.com/{t}/{i}")));
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
                store.shorten(&format!("https://example.com/{i}")),
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
