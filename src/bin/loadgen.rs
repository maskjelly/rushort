//! Bounded, independently scheduled load plus an explicitly labeled pipeline microbenchmark.
use hdrhistogram::Histogram;
use std::{collections::HashSet, io, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{Barrier, mpsc},
    time::{Instant, sleep_until, timeout},
};

#[derive(Clone)]
struct Config {
    target: String,
    host: String,
    port: u16,
    rps: u64,
    duration: Duration,
    connections: usize,
    pipeline: usize,
    queue: usize,
    writes: f64,
    misses: f64,
    seed: usize,
    preloaded: bool,
    rate: bool,
    timeout: Duration,
    json: Option<String>,
    max_p99_ms: Option<f64>,
    key: Option<String>,
}
fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}
fn args() -> io::Result<Config> {
    let mut c = Config {
        target: "http://127.0.0.1:8080".into(),
        host: String::new(),
        port: 8080,
        rps: 2000,
        duration: Duration::from_secs(10),
        connections: 64,
        pipeline: 1,
        queue: 32,
        writes: 0.,
        misses: 0.,
        seed: 1000,
        preloaded: false,
        rate: true,
        timeout: Duration::from_secs(5),
        json: None,
        max_p99_ms: None,
        key: std::env::var("RUSHORT_API_KEY").ok(),
    };
    let mut a = std::env::args().skip(1);
    while let Some(flag) = a.next() {
        if flag == "--help" || flag == "-h" {
            println!(
                "loadgen [--target http://127.0.0.1:8080] [--mode rate|saturate]\n  --rps 2000 --duration 10 --connections 64 --pipeline 1 --queue 32\n  --seed 1000 --preloaded --write-ratio 0 --miss-ratio 0\n  --timeout 5 --max-p99-ms N --json result.json\nPipeline >1 is GET-only, independently randomized per request. Latency is full batch completion.\nRate arrivals run independently of responses; bounded queues reject overload and fail the run.\nAll writes and redirect targets are verified. RUSHORT_API_KEY supplies authentication."
            );
            std::process::exit(0);
        }
        if flag == "--preloaded" {
            c.preloaded = true;
            continue;
        }
        let v = a
            .next()
            .ok_or_else(|| invalid(format!("missing value for {flag}")))?;
        match flag.as_str() {
            "--target" => c.target = v,
            "--mode" => {
                c.rate = match v.as_str() {
                    "rate" => true,
                    "saturate" => false,
                    _ => return Err(invalid("mode must be rate or saturate")),
                }
            }
            "--rps" => c.rps = parse(&v)?,
            "--duration" => c.duration = seconds(&v)?,
            "--connections" => c.connections = parse(&v)?,
            "--pipeline" => c.pipeline = parse(&v)?,
            "--queue" => c.queue = parse(&v)?,
            "--seed" => c.seed = parse(&v)?,
            "--write-ratio" => c.writes = parse(&v)?,
            "--miss-ratio" => c.misses = parse(&v)?,
            "--timeout" => c.timeout = seconds(&v)?,
            "--max-p99-ms" => c.max_p99_ms = Some(parse(&v)?),
            "--json" => c.json = Some(v),
            _ => return Err(invalid(format!("unknown option {flag}"))),
        }
    }
    if !(1..=4096).contains(&c.connections)
        || !(1..=1024).contains(&c.pipeline)
        || !(1..=1024).contains(&c.queue)
        || !(1..=1_000_000).contains(&c.seed)
        || !(1..=1_000_000_000).contains(&c.rps)
        || !(0.0..=1.0).contains(&c.writes)
        || !(0.0..=1.0).contains(&c.misses)
        || c.writes + c.misses > 1.0
        || c.max_p99_ms.is_some_and(|n| !n.is_finite() || n <= 0.)
    {
        return Err(invalid("invalid bounds, ratios or latency threshold"));
    }
    if c.pipeline > 1 && c.writes > 0. {
        return Err(invalid("pipeline >1 requires --write-ratio 0"));
    }
    if (c.rps as f64 * c.duration.as_secs_f64()).round() < 1. {
        return Err(invalid(
            "rate and duration must schedule at least one request",
        ));
    }
    let u = url::Url::parse(&c.target).map_err(|e| invalid(e.to_string()))?;
    if u.scheme() != "http"
        || u.host_str().is_none()
        || u.path() != "/"
        || u.query().is_some()
        || u.fragment().is_some()
        || !u.username().is_empty()
        || u.password().is_some()
    {
        return Err(invalid("target must be an http origin"));
    }
    c.host = u.host_str().unwrap().trim_matches(['[', ']']).to_owned();
    c.port = u.port_or_known_default().unwrap();
    c.target = c.target.trim_end_matches('/').to_owned();
    Ok(c)
}
fn parse<T: std::str::FromStr>(s: &str) -> io::Result<T> {
    s.parse().map_err(|_| invalid("invalid numeric argument"))
}
fn seconds(s: &str) -> io::Result<Duration> {
    let n: f64 = parse(s)?;
    if !n.is_finite() || n <= 0. || n > 86400. {
        return Err(invalid("seconds must be finite and within (0,86400]"));
    }
    Ok(Duration::from_secs_f64(n))
}
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn chance(&mut self, p: f64) -> bool {
        (self.next() >> 11) as f64 / ((1u64 << 53) as f64) < p
    }
}
struct Seed {
    code: String,
    request: Vec<u8>,
    url: String,
}
struct Response {
    status: u16,
    location: String,
    body: Vec<u8>,
}
struct Conn {
    stream: TcpStream,
    buf: Vec<u8>,
    offset: usize,
    tmp: Vec<u8>,
}
impl Conn {
    async fn connect(c: &Config) -> io::Result<Self> {
        let stream = timeout(c.timeout, TcpStream::connect((c.host.as_str(), c.port))).await??;
        stream.set_nodelay(true)?;
        Ok(Self {
            stream,
            buf: Vec::with_capacity(16384),
            offset: 0,
            tmp: vec![0; 16384],
        })
    }
    async fn response(&mut self) -> io::Result<Response> {
        loop {
            let available = &self.buf[self.offset..];
            if let Some(h) = memchr::memmem::find(available, b"\r\n\r\n") {
                if h > 16384 {
                    return Err(invalid("response head too large"));
                }
                let mut headers = [httparse::EMPTY_HEADER; 32];
                let mut res = httparse::Response::new(&mut headers);
                res.parse(&available[..h + 4])
                    .map_err(|_| invalid("bad response"))?;
                let status = res.code.ok_or_else(|| invalid("missing status"))?;
                let mut length = None;
                let mut location = None;
                for hdr in res.headers.iter() {
                    if hdr.name.eq_ignore_ascii_case("content-length") {
                        if length.is_some() {
                            return Err(invalid("duplicate response length"));
                        }
                        length = Some(parse::<usize>(
                            std::str::from_utf8(hdr.value).map_err(|_| invalid("bad length"))?,
                        )?);
                    }
                    if hdr.name.eq_ignore_ascii_case("transfer-encoding") {
                        return Err(invalid("transfer encoding unsupported"));
                    }
                    if hdr.name.eq_ignore_ascii_case("location") {
                        if location.is_some() {
                            return Err(invalid("duplicate location"));
                        }
                        location = Some(
                            std::str::from_utf8(hdr.value)
                                .map_err(|_| invalid("invalid location"))?,
                        );
                    }
                }
                let n = length.ok_or_else(|| invalid("response requires content-length"))?;
                if n > 65536 {
                    return Err(invalid("response body too large"));
                }
                if available.len() >= h + 4 + n {
                    let r = Response {
                        status,
                        location: location.unwrap_or("").into(),
                        body: available[h + 4..h + 4 + n].to_vec(),
                    };
                    self.offset += h + 4 + n;
                    return Ok(r);
                }
            } else if available.len() > 16384 {
                return Err(invalid("response head too large"));
            }
            if self.offset > 0 {
                self.buf.drain(..self.offset);
                self.offset = 0;
            }
            let n = self.stream.read(&mut self.tmp).await?;
            if n == 0 {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }
            self.buf.extend_from_slice(&self.tmp[..n]);
        }
    }
    async fn one(&mut self, req: &[u8]) -> io::Result<Response> {
        self.stream.write_all(req).await?;
        self.response().await
    }
}
fn request(c: &Config, method: &str, path: &str, body: &str) -> Vec<u8> {
    let authority = c.target.strip_prefix("http://").unwrap();
    let mut r = format!("{method} {path} HTTP/1.1\r\nHost: {authority}\r\n");
    if method == "POST" {
        r.push_str(&format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            body.len()
        ));
        if let Some(key) = &c.key {
            r.push_str(&format!("Authorization: Bearer {key}\r\n"));
        }
    }
    r.push_str("\r\n");
    r.push_str(body);
    r.into_bytes()
}
fn shortened(r: &Response, expected: &str) -> io::Result<String> {
    if r.status != 201 {
        return Err(invalid(format!("POST status {}", r.status)));
    }
    let v: serde_json::Value =
        serde_json::from_slice(&r.body).map_err(|_| invalid("invalid POST JSON"))?;
    let code = v["code"].as_str().ok_or_else(|| invalid("missing code"))?;
    if v["long_url"].as_str() != Some(expected)
        || rushort::base62_decode(code).is_none()
        || code.len() > 11
        || (code.len() > 1 && code.starts_with('0'))
    {
        return Err(invalid("POST mapping mismatch"));
    }
    Ok(code.to_owned())
}
async fn seeds(c: &Config) -> io::Result<Vec<Seed>> {
    let mut conn = Conn::connect(c).await?;
    let mut seen = HashSet::new();
    let mut result = Vec::with_capacity(c.seed);
    for i in 0..c.seed {
        let url = format!("https://example.com/seed/{i}");
        let code = if c.preloaded {
            rushort::base62_encode(i as u64)
        } else {
            let req = request(
                c,
                "POST",
                "/api/shorten",
                &serde_json::json!({"url":url}).to_string(),
            );
            let res = timeout(c.timeout, conn.one(&req)).await??;
            shortened(&res, &url)?
        };
        if !seen.insert(code.clone()) {
            return Err(invalid("duplicate seed code"));
        }
        let req = request(c, "GET", &format!("/{code}"), "");
        let res = timeout(c.timeout, conn.one(&req)).await??;
        if res.status != 302 || res.location != url {
            return Err(invalid("seed redirect mismatch"));
        }
        result.push(Seed {
            code,
            request: req,
            url,
        });
    }
    Ok(result)
}
fn hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 86_400_000_000, 3).unwrap()
}
struct Stats {
    sent: u64,
    ok: u64,
    reads: u64,
    writes: u64,
    misses: u64,
    status: u64,
    mismatch: u64,
    errors: u64,
    lat: Histogram<u64>,
    lag: Histogram<u64>,
    created: Vec<(String, String)>,
    first_error: Option<String>,
}
impl Default for Stats {
    fn default() -> Self {
        Self {
            sent: 0,
            ok: 0,
            reads: 0,
            writes: 0,
            misses: 0,
            status: 0,
            mismatch: 0,
            errors: 0,
            lat: hist(),
            lag: hist(),
            created: Vec::new(),
            first_error: None,
        }
    }
}
impl Stats {
    fn note(&mut self, e: impl ToString) {
        if self.first_error.is_none() {
            self.first_error = Some(e.to_string());
        }
    }
    fn merge(&mut self, s: Self) {
        self.sent += s.sent;
        self.ok += s.ok;
        self.reads += s.reads;
        self.writes += s.writes;
        self.misses += s.misses;
        self.status += s.status;
        self.mismatch += s.mismatch;
        self.errors += s.errors;
        self.lat.add(s.lat).unwrap();
        self.lag.add(s.lag).unwrap();
        self.created.extend(s.created);
        if self.first_error.is_none() {
            self.first_error = s.first_error;
        }
    }
}
#[derive(Clone, Copy)]
struct Job {
    sequence: u64,
    count: usize,
    deadline: Instant,
}
struct Worker {
    conn: Option<Conn>,
    rng: Rng,
    stats: Stats,
    request: Vec<u8>,
    expected: Vec<Option<usize>>,
}
impl Worker {
    async fn step(&mut self, c: &Config, pool: &[Seed], job: Job, id: usize) {
        self.stats.sent += job.count as u64;
        let start = Instant::now();
        let lag = start.saturating_duration_since(job.deadline);
        let _ = self
            .stats
            .lag
            .record(lag.as_micros().min(86_400_000_000) as u64);
        let outcome = timeout(c.timeout, self.exchange(c, pool, job, id)).await;
        // Includes scheduling delay; for pipelines this is full batch completion, not divided by N.
        let us = Instant::now()
            .saturating_duration_since(job.deadline)
            .as_micros()
            .min(86_400_000_000) as u64;
        let _ = self.stats.lat.record(us);
        match outcome {
            Ok(Ok(())) => {}
            other => {
                self.stats.note(match other {
                    Ok(Err(e)) => e.to_string(),
                    _ => "request/batch timed out".into(),
                });
                self.conn = None;
            }
        }
    }
    async fn exchange(&mut self, c: &Config, pool: &[Seed], job: Job, id: usize) -> io::Result<()> {
        if self.conn.is_none() {
            self.conn = Some(Conn::connect(c).await?);
        }
        let conn = self.conn.as_mut().unwrap();
        if c.pipeline == 1 && self.rng.chance(c.writes) {
            if self.stats.created.len() >= 1_000_000 / c.connections {
                return Err(invalid("verification capacity reached; shorten run"));
            }
            let url = format!("https://example.com/load/{id}/{}", job.sequence);
            let req = request(
                c,
                "POST",
                "/api/shorten",
                &serde_json::json!({"url":url}).to_string(),
            );
            let r = conn.one(&req).await?;
            if r.status != 201 {
                self.stats.status += 1;
                self.stats.note(format!("POST status {}", r.status));
                return Ok(());
            }
            match shortened(&r, &url) {
                Ok(code) => {
                    self.stats.ok += 1;
                    self.stats.writes += 1;
                    self.stats.created.push((code, url));
                }
                Err(e) => {
                    self.stats.mismatch += 1;
                    self.stats.note(e);
                }
            }
            return Ok(());
        }
        self.request.clear();
        self.expected.clear();
        for _ in 0..job.count {
            if self.rng.chance(c.misses / (1.0 - c.writes)) {
                self.request
                    .extend_from_slice(&request(c, "GET", "/missing-code", ""));
                self.expected.push(None);
            } else {
                let idx = self.rng.next() as usize % pool.len();
                self.request.extend_from_slice(&pool[idx].request);
                self.expected.push(Some(idx));
            }
        }
        conn.stream.write_all(&self.request).await?;
        for expected in &self.expected {
            let r = conn.response().await?;
            match expected {
                Some(_) if r.status != 302 => self.stats.status += 1,
                Some(idx) if r.location != pool[*idx].url => self.stats.mismatch += 1,
                Some(_) => {
                    self.stats.ok += 1;
                    self.stats.reads += 1;
                }
                None if r.status != 404 => self.stats.status += 1,
                None => {
                    self.stats.ok += 1;
                    self.stats.misses += 1;
                }
            }
        }
        Ok(())
    }
}
async fn verify(c: &Config, pool: &[Seed], created: &[(String, String)]) -> io::Result<usize> {
    let mut codes: HashSet<&str> = pool.iter().map(|s| s.code.as_str()).collect();
    let mut conn = Conn::connect(c).await?;
    for (code, url) in created {
        if !codes.insert(code.as_str()) {
            return Err(invalid("duplicate code across writes"));
        }
        let res = timeout(
            c.timeout,
            conn.one(&request(c, "GET", &format!("/{code}"), "")),
        )
        .await??;
        if res.status != 302 || res.location != *url {
            return Err(invalid("post-run redirect mismatch"));
        }
    }
    for seed in pool {
        let res = timeout(c.timeout, conn.one(&seed.request)).await??;
        if res.status != 302 || res.location != seed.url {
            return Err(invalid("seed mapping changed during run"));
        }
    }
    Ok(created.len())
}
async fn run(c: Config) -> io::Result<bool> {
    let pool = Arc::new(seeds(&c).await?);
    eprintln!(
        "loadgen: verified {} seeds; preparing connections",
        pool.len()
    );
    // Connections are established before the shared start; no hidden warm-up traffic is counted.
    let mut connections = Vec::new();
    for _ in 0..c.connections {
        connections.push(Conn::connect(&c).await?);
    }
    let start = Instant::now() + Duration::from_millis(100);
    let end = start + c.duration;
    let total = (c.rps as f64 * c.duration.as_secs_f64()).round() as u64;
    let mut tasks = Vec::new();
    let mut producers = Vec::new();
    let barrier = Arc::new(Barrier::new(c.connections + 1));
    for (id, conn) in connections.into_iter().enumerate() {
        let cfg = c.clone();
        let pool = pool.clone();
        let barrier = barrier.clone();
        let (tx, mut rx) = mpsc::channel::<Job>(c.queue);
        if c.rate {
            let cfg = cfg.clone();
            producers.push(tokio::spawn(async move {
                let mut dropped = 0u64;
                let stride = cfg.connections as u64 * cfg.pipeline as u64;
                let mut sequence = id as u64 * cfg.pipeline as u64;
                while sequence < total {
                    let deadline =
                        start + Duration::from_secs_f64(sequence as f64 / cfg.rps as f64);
                    sleep_until(deadline).await;
                    let count = (total - sequence).min(cfg.pipeline as u64) as usize;
                    if tx
                        .try_send(Job {
                            sequence,
                            count,
                            deadline,
                        })
                        .is_err()
                    {
                        dropped += count as u64;
                    }
                    sequence += stride;
                }
                dropped
            }));
        } else {
            drop(tx);
        }
        tasks.push(tokio::spawn(async move {
            let mut worker = Worker {
                conn: Some(conn),
                rng: Rng(0x9e3779b97f4a7c15u64.wrapping_mul(id as u64 + 1)),
                stats: Stats::default(),
                request: Vec::new(),
                expected: Vec::new(),
            };
            barrier.wait().await;
            sleep_until(start).await;
            if cfg.rate {
                while let Some(job) = rx.recv().await {
                    worker.step(&cfg, &pool, job, id).await;
                }
            } else {
                let mut sequence = 0;
                while Instant::now() < end {
                    let job = Job {
                        sequence,
                        count: cfg.pipeline,
                        deadline: Instant::now(),
                    };
                    worker.step(&cfg, &pool, job, id).await;
                    sequence += cfg.pipeline as u64;
                }
            }
            // Every attempted request has exactly one accounting outcome, including partial batches.
            worker.stats.errors =
                worker.stats.sent - worker.stats.ok - worker.stats.status - worker.stats.mismatch;
            worker.stats
        }));
    }
    barrier.wait().await;
    let mut dropped = 0;
    for task in producers {
        dropped += task.await.map_err(io::Error::other)?;
    }
    let mut stats = Stats::default();
    for task in tasks {
        stats.merge(task.await.map_err(io::Error::other)?);
    }
    sleep_until(end).await;
    let elapsed = Instant::now().duration_since(start);
    let verification = verify(&c, &pool, &stats.created).await;
    let verified = verification.as_ref().copied().unwrap_or(0);
    let achieved = stats.ok as f64 / elapsed.as_secs_f64();
    let p99 = stats.lat.value_at_quantile(0.99) as f64 / 1000.;
    let accounting = !c.rate || stats.sent + dropped == total;
    let pass = stats.ok > 0
        && stats.errors == 0
        && stats.status == 0
        && stats.mismatch == 0
        && dropped == 0
        && accounting
        && verification.is_ok()
        && (!c.rate || (stats.ok == total && achieved >= c.rps as f64 * 0.99))
        && c.max_p99_ms.is_none_or(|limit| p99 <= limit);
    let report = serde_json::json!({"pass":pass,"mode":if c.rate{"rate"}else{"saturate"},"target":c.target,"duration_requested_s":c.duration.as_secs_f64(),"elapsed_s":elapsed.as_secs_f64(),"target_rps":if c.rate{Some(c.rps)}else{None},"planned":if c.rate{Some(total)}else{None},"attempted":stats.sent,"ok":stats.ok,"dropped":dropped,"transport_errors":stats.errors,"unexpected_status":stats.status,"mismatches":stats.mismatch,"reads":stats.reads,"writes":stats.writes,"misses":stats.misses,"verified_writes":verified,"verification_error":verification.err().map(|e|e.to_string()),"rps":achieved,"pipeline":c.pipeline,"queue_batches_per_connection":c.queue,"connections":c.connections,"seed":c.seed,"write_ratio":c.writes,"miss_ratio":c.misses,"latency_kind":if c.pipeline>1{"scheduled batch completion"}else{"scheduled request completion"},"p50_ms":stats.lat.value_at_quantile(0.5) as f64/1000.,"p99_ms":p99,"max_ms":stats.lat.max() as f64/1000.,"max_send_lag_ms":stats.lag.max() as f64/1000.,"first_error":stats.first_error});
    println!("{}", serde_json::to_string_pretty(&report)?);
    println!("RESULT: {}", if pass { "PASS" } else { "FAIL" });
    if let Some(path) = c.json {
        std::fs::write(path, serde_json::to_string_pretty(&report)?)?;
    }
    Ok(pass)
}
#[tokio::main]
async fn main() {
    let result = match args() {
        Ok(c) => run(c).await,
        Err(e) => Err(e),
    };
    match result {
        Ok(true) => {}
        Ok(false) => std::process::exit(1),
        Err(e) => {
            eprintln!("loadgen: {e}");
            std::process::exit(1);
        }
    }
}
