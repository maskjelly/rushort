use std::io::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Rate,
    Saturate,
}

#[derive(Clone)]
struct Config {
    host: String,
    port: u16,
    rps: u64,
    duration: Duration,
    connections: usize,
    write_ratio: f64,
    mode: Mode,
    timeout: Duration,
    seed: usize,
    pipeline: usize,
}

fn usage() -> ! {
    println!(
        "rushort load simulator\n\n\
         USAGE: loadgen [OPTIONS]\n\n\
         OPTIONS:\n  \
         --target URL       base target, http only (default http://127.0.0.1:8080)\n  \
         --rps N            request rate to sustain in rate mode (default 2000)\n  \
         --duration SECS    test duration, fractional ok (default 10)\n  \
         --connections N    concurrent keep-alive connections (default 64)\n  \
         --write-ratio F    fraction of POST /api/shorten requests, 0..1 (default 0.05)\n  \
         --mode MODE        rate | saturate (default rate)\n  \
         --timeout SECS     per-request timeout (default 5)\n  \
         --seed N           URLs pre-created for the read pool (default 1000)\n  \
         --pipeline N       requests in flight per connection, saturate mode only,\n\
         \x20                  GET-only, amortizes transport cost (default 1)\n  \
         -h, --help         show this help\n\n\
         rate mode schedules exactly rps*duration requests (open loop) and fails\n\
         unless achieved rate >= 99% of target with zero errors and zero\n\
         verification mismatches. saturate mode sends as fast as possible for the\n\
         duration to show headroom; --pipeline raises it to measure request\n\
         processing throughput rather than TCP round-trip cost."
    );
    std::process::exit(0);
}

fn parse_args() -> Config {
    let mut cfg = Config {
        host: "127.0.0.1".to_owned(),
        port: 8080,
        rps: 2000,
        duration: Duration::from_secs(10),
        connections: 64,
        write_ratio: 0.05,
        mode: Mode::Rate,
        timeout: Duration::from_secs(5),
        seed: 1000,
        pipeline: 1,
    };

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--target" => {
                let target = next_val(&mut args, &arg);
                let (host, port) = parse_target(&target);
                cfg.host = host;
                cfg.port = port;
            }
            "--rps" => cfg.rps = parse_val(&mut args, &arg),
            "--duration" => cfg.duration = Duration::from_secs_f64(parse_val(&mut args, &arg)),
            "--connections" => cfg.connections = parse_val(&mut args, &arg),
            "--write-ratio" => cfg.write_ratio = parse_val(&mut args, &arg),
            "--timeout" => cfg.timeout = Duration::from_secs_f64(parse_val(&mut args, &arg)),
            "--seed" => cfg.seed = parse_val(&mut args, &arg),
            "--pipeline" => cfg.pipeline = parse_val::<usize>(&mut args, &arg).max(1),
            "--mode" => {
                let mode = next_val(&mut args, &arg);
                cfg.mode = match mode.as_str() {
                    "rate" => Mode::Rate,
                    "saturate" => Mode::Saturate,
                    other => {
                        eprintln!("loadgen: unknown mode `{other}` (rate | saturate)");
                        std::process::exit(2);
                    }
                };
            }
            "-h" | "--help" => usage(),
            other => {
                eprintln!("loadgen: unknown argument `{other}` (try --help)");
                std::process::exit(2);
            }
        }
    }

    if cfg.connections == 0 {
        eprintln!("loadgen: --connections must be > 0");
        std::process::exit(2);
    }
    if cfg.seed == 0 && cfg.write_ratio <= 0.0 {
        eprintln!("loadgen: need either --seed > 0 or --write-ratio > 0");
        std::process::exit(2);
    }
    if !(0.0..=1.0).contains(&cfg.write_ratio) {
        eprintln!("loadgen: --write-ratio must be between 0 and 1");
        std::process::exit(2);
    }
    cfg
}

fn next_val(args: &mut impl Iterator<Item = String>, flag: &str) -> String {
    args.next().unwrap_or_else(|| {
        eprintln!("loadgen: {flag} requires a value");
        std::process::exit(2);
    })
}

fn parse_val<T: std::str::FromStr>(args: &mut impl Iterator<Item = String>, flag: &str) -> T {
    let raw = next_val(args, flag);
    raw.parse().unwrap_or_else(|_| {
        eprintln!("loadgen: invalid value `{raw}` for {flag}");
        std::process::exit(2);
    })
}

fn parse_target(target: &str) -> (String, u16) {
    let rest = target.strip_prefix("http://").unwrap_or_else(|| {
        eprintln!("loadgen: only http:// targets are supported, got `{target}`");
        std::process::exit(2);
    });
    let authority = rest.split('/').next().unwrap_or(rest);
    match authority.rsplit_once(':') {
        Some((host, port)) => (
            host.to_owned(),
            port.parse().unwrap_or_else(|_| {
                eprintln!("loadgen: invalid port in `{target}`");
                std::process::exit(2);
            }),
        ),
        None => (authority.to_owned(), 80),
    }
}

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    #[inline]
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    #[inline]
    fn chance(&mut self, p: f64) -> bool {
        if p <= 0.0 {
            return false;
        }
        if p >= 1.0 {
            return true;
        }
        ((self.next() >> 11) as f64 / (1u64 << 53) as f64) < p
    }
}

struct Response {
    status: u16,
    location: Option<String>,
    body: Vec<u8>,
}

struct Conn {
    stream: TcpStream,
    authority: String,
    buf: Vec<u8>,
    req: Vec<u8>,
    tmp: Vec<u8>,
}

impl Conn {
    async fn connect(host: &str, port: u16) -> std::io::Result<Self> {
        let stream = TcpStream::connect((host, port)).await?;
        stream.set_nodelay(true)?;
        Ok(Self {
            stream,
            authority: format!("{host}:{port}"),
            buf: Vec::with_capacity(8192),
            req: Vec::with_capacity(512),
            tmp: vec![0u8; 16 * 1024],
        })
    }

    async fn get(&mut self, path: &str) -> std::io::Result<Response> {
        self.req.clear();
        write!(
            self.req,
            "GET {path} HTTP/1.1\r\nHost: {}\r\nAccept: */*\r\nConnection: keep-alive\r\n\r\n",
            self.authority
        )
        .unwrap();
        self.stream.write_all(&self.req).await?;
        self.read_response().await
    }

    async fn pipeline_get(
        &mut self,
        path: &str,
        count: usize,
        expected: &str,
    ) -> std::io::Result<(usize, usize)> {
        self.req.clear();
        for _ in 0..count {
            write!(
                self.req,
                "GET {path} HTTP/1.1\r\nHost: {}\r\nAccept: */*\r\nConnection: keep-alive\r\n\r\n",
                self.authority
            )
            .unwrap();
        }
        self.stream.write_all(&self.req).await?;
        let mut ok = 0;
        let mut mismatched = 0;
        for _ in 0..count {
            let resp = self.read_response().await?;
            if resp.status == 302 && resp.location.as_deref() == Some(expected) {
                ok += 1;
            } else {
                mismatched += 1;
            }
        }
        Ok((ok, mismatched))
    }

    async fn post_json(&mut self, path: &str, body: &[u8]) -> std::io::Result<Response> {
        self.req.clear();
        write!(
            self.req,
            "POST {path} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
            self.authority,
            body.len()
        )
        .unwrap();
        self.req.extend_from_slice(body);
        self.stream.write_all(&self.req).await?;
        self.read_response().await
    }

    async fn read_response(&mut self) -> std::io::Result<Response> {
        loop {
            if let Some((resp, used)) = parse_response(&self.buf)? {
                self.buf.drain(..used);
                return Ok(resp);
            }
            let n = self.stream.read(&mut self.tmp).await?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed by peer",
                ));
            }
            self.buf.extend_from_slice(&self.tmp[..n]);
        }
    }
}

fn invalid(msg: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg.to_owned())
}

fn parse_response(buf: &[u8]) -> std::io::Result<Option<(Response, usize)>> {
    let Some(hdr_end) = find_subslice(buf, b"\r\n\r\n") else {
        return Ok(None);
    };
    let head = &buf[..hdr_end];
    let mut lines = head.split(|&b| b == b'\n');
    let status =
        parse_status(lines.next().unwrap_or(b"")).ok_or_else(|| invalid("bad status line"))?;

    let mut content_length = 0usize;
    let mut location = None;
    for line in lines {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let Some(colon) = line.iter().position(|&b| b == b':') else {
            continue;
        };
        let (name, value) = line.split_at(colon);
        let value = trim_ascii(&value[1..]);
        if name.eq_ignore_ascii_case(b"content-length") {
            content_length = std::str::from_utf8(value)
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| invalid("bad content-length"))?;
        } else if name.eq_ignore_ascii_case(b"transfer-encoding") {
            return Err(invalid("chunked responses are not supported by loadgen"));
        } else if name.eq_ignore_ascii_case(b"location") {
            location = Some(String::from_utf8_lossy(value).into_owned());
        }
    }

    let end = hdr_end + 4 + content_length;
    if buf.len() < end {
        return Ok(None);
    }
    Ok(Some((
        Response {
            status,
            location,
            body: buf[hdr_end + 4..end].to_vec(),
        },
        end,
    )))
}

fn parse_status(line: &[u8]) -> Option<u16> {
    let mut parts = line.split(|&b| b == b' ');
    let _version = parts.next()?;
    let code = parts.next()?;
    std::str::from_utf8(code).ok()?.parse().ok()
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn trim_ascii(mut b: &[u8]) -> &[u8] {
    while let Some((first, rest)) = b.split_first() {
        if *first == b' ' || *first == b'\t' {
            b = rest;
        } else {
            break;
        }
    }
    while let Some((last, rest)) = b.split_last() {
        if *last == b' ' || *last == b'\t' || *last == b'\r' {
            b = rest;
        } else {
            break;
        }
    }
    b
}

fn parse_shorten_out(body: &[u8]) -> Result<(String, String), String> {
    let value: serde_json::Value =
        serde_json::from_slice(body).map_err(|e| format!("bad JSON: {e}"))?;
    let code = value
        .get("code")
        .and_then(|v| v.as_str())
        .ok_or("missing `code`")?
        .to_owned();
    let long_url = value
        .get("long_url")
        .and_then(|v| v.as_str())
        .ok_or("missing `long_url`")?
        .to_owned();
    Ok((code, long_url))
}

#[derive(Default)]
struct TaskResult {
    read_lat: Vec<u32>,
    write_lat: Vec<u32>,
    ok_reads: u64,
    ok_writes: u64,
    status_other: u64,
    errors: u64,
    verify_fail: u64,
    max_lag_us: u64,
    writes: Vec<(String, String)>,
    samples: Vec<String>,
}

impl TaskResult {
    fn note(&mut self, msg: String) {
        if self.samples.len() < 8 {
            self.samples.push(msg);
        }
    }
}

struct Task {
    conn: Conn,
    cfg: Config,
    seeds: Arc<Vec<(String, String)>>,
    local: Vec<(String, String)>,
    rng: Rng,
    res: TaskResult,
}

impl Task {
    fn new(conn: Conn, cfg: Config, seeds: Arc<Vec<(String, String)>>, id: usize) -> Self {
        let seed = 0x9e37_79b9_7f4a_7c15u64
            .wrapping_mul(id as u64 + 1)
            .wrapping_add(0xdead_beef);
        Self {
            conn,
            cfg,
            seeds,
            local: Vec::new(),
            rng: Rng::new(seed),
            res: TaskResult::default(),
        }
    }

    fn finish(mut self) -> TaskResult {
        self.res.writes = std::mem::take(&mut self.local);
        self.res
    }

    async fn step(&mut self, id: usize, seq: u64) {
        if self.cfg.pipeline > 1 {
            self.pipelined().await;
        } else if self.rng.chance(self.cfg.write_ratio) {
            self.write(id, seq).await;
        } else {
            self.read().await;
        }
    }

    async fn pipelined(&mut self) {
        let (code, expected) = self.pick();
        let path = format!("/{code}");
        let depth = self.cfg.pipeline;
        let t0 = Instant::now();
        match tokio::time::timeout(
            self.cfg.timeout,
            self.conn.pipeline_get(&path, depth, &expected),
        )
        .await
        {
            Ok(Ok((ok, mismatched))) => {
                self.res.read_lat.push(elapsed_us(t0) / depth as u32);
                self.res.ok_reads += ok as u64;
                self.res.verify_fail += mismatched as u64;
            }
            Ok(Err(e)) => {
                self.res.errors += 1;
                self.res.note(format!("pipeline GET failed: {e}"));
                self.reconnect().await;
            }
            Err(_) => {
                self.res.errors += 1;
                self.res.note("pipeline GET timed out".to_owned());
                self.reconnect().await;
            }
        }
    }

    async fn read(&mut self) {
        let (code, expected) = self.pick();
        let path = format!("/{code}");
        let t0 = Instant::now();
        match tokio::time::timeout(self.cfg.timeout, self.conn.get(&path)).await {
            Ok(Ok(resp)) if resp.status == 302 => {
                self.res.read_lat.push(elapsed_us(t0));
                if resp.location.as_deref() == Some(expected.as_str()) {
                    self.res.ok_reads += 1;
                } else {
                    self.res.verify_fail += 1;
                    self.res.note(format!(
                        "location mismatch for `{code}`: expected `{expected}`, got `{:?}`",
                        resp.location
                    ));
                }
            }
            Ok(Ok(resp)) => {
                self.res.status_other += 1;
                self.res
                    .note(format!("unexpected status {} for GET /{code}", resp.status));
            }
            Ok(Err(e)) => {
                self.res.errors += 1;
                self.res.note(format!("GET /{code} failed: {e}"));
                self.reconnect().await;
            }
            Err(_) => {
                self.res.errors += 1;
                self.res.note(format!("GET /{code} timed out"));
                self.reconnect().await;
            }
        }
    }

    async fn write(&mut self, id: usize, seq: u64) {
        let url = format!("https://example.com/load/{id}/{seq}");
        let body = format!("{{\"url\":\"{url}\"}}");
        let t0 = Instant::now();
        match tokio::time::timeout(
            self.cfg.timeout,
            self.conn.post_json("/api/shorten", body.as_bytes()),
        )
        .await
        {
            Ok(Ok(resp)) if resp.status == 201 => {
                self.res.write_lat.push(elapsed_us(t0));
                match parse_shorten_out(&resp.body) {
                    Ok((code, long_url)) => {
                        self.res.ok_writes += 1;
                        self.local.push((code, long_url));
                    }
                    Err(e) => {
                        self.res.status_other += 1;
                        self.res.note(format!("bad JSON on POST: {e}"));
                    }
                }
            }
            Ok(Ok(resp)) => {
                self.res.status_other += 1;
                self.res.note(format!(
                    "unexpected status {} on POST /api/shorten",
                    resp.status
                ));
            }
            Ok(Err(e)) => {
                self.res.errors += 1;
                self.res.note(format!("POST failed: {e}"));
                self.reconnect().await;
            }
            Err(_) => {
                self.res.errors += 1;
                self.res.note("POST timed out".to_owned());
                self.reconnect().await;
            }
        }
    }

    fn pick(&mut self) -> (String, String) {
        let r = self.rng.next();
        let use_local = !self.local.is_empty() && (r % 100) < 80;
        let src = if use_local { &self.local } else { &self.seeds };
        let idx = ((r >> 8) as usize) % src.len();
        (src[idx].0.clone(), src[idx].1.clone())
    }

    async fn reconnect(&mut self) {
        for attempt in 1..=5 {
            match Conn::connect(&self.cfg.host, self.cfg.port).await {
                Ok(conn) => {
                    self.conn = conn;
                    return;
                }
                Err(e) => {
                    self.res
                        .note(format!("reconnect attempt {attempt} failed: {e}"));
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            }
        }
        eprintln!(
            "loadgen: FATAL: cannot reconnect to {}:{}",
            self.cfg.host, self.cfg.port
        );
        std::process::exit(1);
    }
}

fn elapsed_us(t0: Instant) -> u32 {
    t0.elapsed().as_micros().min(u32::MAX as u128) as u32
}

async fn seed_pool(cfg: &Config) -> Vec<(String, String)> {
    let mut conn = match Conn::connect(&cfg.host, cfg.port).await {
        Ok(conn) => conn,
        Err(e) => {
            eprintln!("loadgen: cannot connect to {}:{}: {e}", cfg.host, cfg.port);
            std::process::exit(1);
        }
    };
    let mut pool = Vec::with_capacity(cfg.seed);
    for i in 0..cfg.seed {
        let url = format!("https://example.com/seed/{i}");
        let body = format!("{{\"url\":\"{url}\"}}");
        match tokio::time::timeout(cfg.timeout, conn.post_json("/api/shorten", body.as_bytes()))
            .await
        {
            Ok(Ok(resp)) if resp.status == 201 => match parse_shorten_out(&resp.body) {
                Ok(out) => pool.push(out),
                Err(e) => {
                    eprintln!("loadgen: seed {i}: bad JSON: {e}");
                    std::process::exit(1);
                }
            },
            Ok(Ok(resp)) => {
                eprintln!("loadgen: seed {i}: unexpected status {}", resp.status);
                std::process::exit(1);
            }
            Ok(Err(e)) => {
                eprintln!("loadgen: seed {i}: {e}");
                std::process::exit(1);
            }
            Err(_) => {
                eprintln!("loadgen: seed {i}: timed out");
                std::process::exit(1);
            }
        }
    }
    pool
}

async fn run_rate(cfg: &Config, seeds: Arc<Vec<(String, String)>>) {
    let total = (cfg.rps as f64 * cfg.duration.as_secs_f64()).round() as u64;
    let per_step = cfg.pipeline as u64;
    let batches = total.div_ceil(cfg.connections as u64 * per_step).max(1);
    let start = Instant::now() + Duration::from_millis(500);

    let mut handles = Vec::with_capacity(cfg.connections);
    for id in 0..cfg.connections {
        let cfg = cfg.clone();
        let seeds = seeds.clone();
        handles.push(tokio::spawn(async move {
            let mut task = Task::new(connect_or_die(&cfg).await, cfg.clone(), seeds, id);
            let period = cfg.duration.as_secs_f64() / batches as f64;
            for seq in 0..batches {
                let deadline = start + Duration::from_secs_f64(period * seq as f64);
                tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
                let lag = Instant::now().saturating_duration_since(deadline);
                task.res.max_lag_us = task.res.max_lag_us.max(lag.as_micros() as u64);
                task.step(id, seq).await;
            }
            task.finish()
        }));
    }

    let mut results = Vec::with_capacity(handles.len());
    for handle in handles {
        results.push(handle.await.expect("worker task panicked"));
    }
    let elapsed = Instant::now().saturating_duration_since(start);
    report(cfg, results, elapsed, Some(total)).await;
}

async fn run_saturate(cfg: &Config, seeds: Arc<Vec<(String, String)>>) {
    let start = Instant::now() + Duration::from_millis(500);
    let end = start + cfg.duration;

    let mut handles = Vec::with_capacity(cfg.connections);
    for id in 0..cfg.connections {
        let cfg = cfg.clone();
        let seeds = seeds.clone();
        handles.push(tokio::spawn(async move {
            let mut task = Task::new(connect_or_die(&cfg).await, cfg.clone(), seeds, id);
            let mut seq = 0u64;
            while Instant::now() < end {
                task.step(id, seq).await;
                seq += 1;
            }
            task.finish()
        }));
    }

    let mut results = Vec::with_capacity(handles.len());
    for handle in handles {
        results.push(handle.await.expect("worker task panicked"));
    }
    let elapsed = Instant::now().saturating_duration_since(start);
    report(cfg, results, elapsed, None).await;
}

async fn connect_or_die(cfg: &Config) -> Conn {
    match Conn::connect(&cfg.host, cfg.port).await {
        Ok(conn) => conn,
        Err(e) => {
            eprintln!("loadgen: cannot connect to {}:{}: {e}", cfg.host, cfg.port);
            std::process::exit(1);
        }
    }
}

async fn report(
    cfg: &Config,
    results: Vec<TaskResult>,
    elapsed: Duration,
    target_total: Option<u64>,
) {
    let mut read_lat = Vec::new();
    let mut write_lat = Vec::new();
    let mut ok_reads = 0u64;
    let mut ok_writes = 0u64;
    let mut status_other = 0u64;
    let mut errors = 0u64;
    let mut verify_fail = 0u64;
    let mut max_lag_us = 0u64;
    let mut writes_all = Vec::new();
    let mut samples = Vec::new();

    for result in results {
        read_lat.extend_from_slice(&result.read_lat);
        write_lat.extend_from_slice(&result.write_lat);
        ok_reads += result.ok_reads;
        ok_writes += result.ok_writes;
        status_other += result.status_other;
        errors += result.errors;
        verify_fail += result.verify_fail;
        max_lag_us = max_lag_us.max(result.max_lag_us);
        writes_all.extend(result.writes);
        samples.extend(result.samples);
    }

    read_lat.sort_unstable();
    write_lat.sort_unstable();
    let mut all_lat: Vec<u32> = read_lat.iter().chain(write_lat.iter()).copied().collect();
    all_lat.sort_unstable();

    let total_ok = ok_reads + ok_writes;
    let achieved = total_ok as f64 / elapsed.as_secs_f64();

    let mode = match cfg.mode {
        Mode::Rate => format!("rate @ {} req/s", cfg.rps),
        Mode::Saturate => "saturate".to_owned(),
    };
    println!("\n=== rushort load simulator ===");
    println!(
        "target:           http://{}:{}  ({mode})",
        cfg.host, cfg.port
    );
    println!(
        "duration:         {:.2}s   connections: {}   mix: {:.0}% GET / {:.0}% POST",
        elapsed.as_secs_f64(),
        cfg.connections,
        (1.0 - cfg.write_ratio) * 100.0,
        cfg.write_ratio * 100.0
    );
    if cfg.pipeline > 1 {
        println!(
            "pipeline:         depth {} (GET batches, per-request latency estimated from batch time)",
            cfg.pipeline
        );
    }
    println!(
        "requests:         {total_ok} ok  (GET {ok_reads}, POST {ok_writes})   unexpected-status {status_other}   errors {errors}   verify-fail {verify_fail}"
    );
    if let Some(target) = target_total {
        println!(
            "achieved:         {achieved:.1} req/s   plan {target} req   (target {} req/s)",
            cfg.rps
        );
    } else {
        println!("achieved:         {achieved:.1} req/s");
    }
    println!(
        "send lag:         max {:.2}ms (scheduler falling behind)",
        max_lag_us as f64 / 1000.0
    );
    print_latency("latency all ", &all_lat);
    print_latency("latency GET ", &read_lat);
    print_latency("latency POST", &write_lat);

    let (checked_writes, verified_writes, failed_writes) = post_verify(cfg, &writes_all).await;
    println!(
        "post-run verify:  {verified_writes}/{checked_writes} writes re-checked via GET, {failed_writes} wrong"
    );

    if !samples.is_empty() {
        println!("samples:          {}", samples[0]);
        for sample in samples.iter().skip(1).take(4) {
            println!("                  {sample}");
        }
    }

    let rate_ok = match (cfg.mode, target_total) {
        (Mode::Rate, Some(target)) if target > 0 => achieved >= cfg.rps as f64 * 0.99,
        _ => true,
    };
    let pass = errors == 0 && verify_fail == 0 && failed_writes == 0 && rate_ok;
    if pass {
        match cfg.mode {
            Mode::Rate => println!(
                "\nRESULT: PASS - sustained {achieved:.1} req/s for {:.1}s with 0 errors, 0 mismatches",
                elapsed.as_secs_f64()
            ),
            Mode::Saturate => {
                println!("\nRESULT: PASS - peak {achieved:.1} req/s with 0 errors, 0 mismatches")
            }
        }
    } else {
        println!("\nRESULT: FAIL");
    }
    std::process::exit(if pass { 0 } else { 1 });
}

async fn post_verify(cfg: &Config, writes: &[(String, String)]) -> (usize, usize, usize) {
    if writes.is_empty() {
        return (0, 0, 0);
    }
    let step = (writes.len() / 200).max(1);
    let sample: Vec<&(String, String)> = writes.iter().step_by(step).take(200).collect();

    let mut conn = match Conn::connect(&cfg.host, cfg.port).await {
        Ok(conn) => conn,
        Err(_) => return (sample.len(), 0, sample.len()),
    };
    let mut ok = 0usize;
    let mut bad = 0usize;
    for (code, expected) in &sample {
        let path = format!("/{code}");
        let outcome = tokio::time::timeout(cfg.timeout, conn.get(&path)).await;
        match outcome {
            Ok(Ok(resp))
                if resp.status == 302 && resp.location.as_deref() == Some(expected.as_str()) =>
            {
                ok += 1;
            }
            Ok(Ok(resp)) => {
                bad += 1;
                eprintln!(
                    "loadgen: verify failed for `{code}`: status {} location {:?}",
                    resp.status, resp.location
                );
            }
            other => {
                bad += 1;
                let reason = match other {
                    Err(_) => "timeout",
                    Ok(Err(_)) => "transport/parse error",
                    Ok(Ok(_)) => "unexpected response",
                };
                eprintln!("loadgen: verify failed for `{code}`: {reason}");
                match Conn::connect(&cfg.host, cfg.port).await {
                    Ok(new_conn) => conn = new_conn,
                    Err(_) => return (sample.len(), ok, bad),
                }
            }
        }
    }
    (sample.len(), ok, bad)
}

fn print_latency(label: &str, lat: &[u32]) {
    if lat.is_empty() {
        println!("{label}       (no samples)");
        return;
    }
    println!(
        "{label}       p50 {:.2}ms  p90 {:.2}ms  p99 {:.2}ms  p999 {:.2}ms  max {:.2}ms  (n={})",
        pct(lat, 0.50) as f64 / 1000.0,
        pct(lat, 0.90) as f64 / 1000.0,
        pct(lat, 0.99) as f64 / 1000.0,
        pct(lat, 0.999) as f64 / 1000.0,
        lat[lat.len() - 1] as f64 / 1000.0,
        lat.len()
    );
}

fn pct(sorted: &[u32], p: f64) -> u32 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx]
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let cfg = parse_args();
    let started = Instant::now();
    let seeds = Arc::new(seed_pool(&cfg).await);
    eprintln!(
        "loadgen: seeded {} urls in {:.2}s, starting {} mode",
        seeds.len(),
        started.elapsed().as_secs_f64(),
        match cfg.mode {
            Mode::Rate => "rate",
            Mode::Saturate => "saturate",
        }
    );
    match cfg.mode {
        Mode::Rate => run_rate(&cfg, seeds).await,
        Mode::Saturate => run_saturate(&cfg, seeds).await,
    }
}
