use rushort::{ServerConfig, Store, serve_with_shutdown, validate_url};
use std::{io, net::SocketAddr, path::Path, sync::Arc, time::Duration};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut bind = "127.0.0.1:8080".to_owned();
    let mut database = "rushort.db".to_owned();
    let mut shards = 64usize;
    let mut max_urls = 1_000_000usize;
    let mut preload = 0usize;
    let mut ephemeral = false;
    let mut cfg = ServerConfig::default();
    let mut public_base = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--help" || arg == "-h" {
            println!(
                "shortener [--bind 127.0.0.1:8080] [--db rushort.db] [--public-base https://s.example]\n  --ephemeral               Explicit RAM-only benchmark mode\n  --shards 64               Power of two, 1..4096\n  --max-urls 1000000         Hard storage/cache capacity\n  --max-connections 1024     Hard active connection limit\n  --timeout 5               Header/body and socket-write deadline in seconds\n  --max-writes 32           Concurrent write admission limit\n  --preload 0               Seed deterministic URLs before listening\nRUSHORT_API_KEY: >=32 ASCII characters; required for non-loopback binds.\nUse a TLS reverse proxy for public access. One process per local SQLite file."
            );
            return Ok(());
        }
        if arg == "--ephemeral" {
            ephemeral = true;
            continue;
        }
        let value = args
            .next()
            .ok_or_else(|| io::Error::other(format!("missing value for {arg}")))?;
        match arg.as_str() {
            "--bind" => bind = value,
            "--db" => database = value,
            "--public-base" => public_base = Some(value),
            "--shards" => shards = parse(&value)?,
            "--max-urls" => max_urls = parse(&value)?,
            "--preload" => preload = parse(&value)?,
            "--max-connections" => cfg.max_connections = parse(&value)?,
            "--max-writes" => cfg.max_writes = parse(&value)?,
            "--timeout" => cfg.io_timeout = Duration::from_secs(parse(&value)?),
            _ => return Err(io::Error::other(format!("unknown option {arg}"))),
        }
    }
    let addr: SocketAddr = bind.parse().map_err(io::Error::other)?;
    if !shards.is_power_of_two()
        || shards > 4096
        || max_urls == 0
        || preload > max_urls
        || cfg.max_connections == 0
        || cfg.max_connections > 65536
        || cfg.max_writes == 0
        || cfg.max_writes > 1024
        || cfg.io_timeout.is_zero()
        || cfg.io_timeout.as_secs() > 300
    {
        return Err(io::Error::other("invalid capacity, shard count or timeout"));
    }
    cfg.write_token = std::env::var("RUSHORT_API_KEY").ok();
    if let Some(key) = &cfg.write_token
        && (key.len() < 32 || key.len() > 256 || !key.bytes().all(|b| (0x21..=0x7e).contains(&b)))
    {
        return Err(io::Error::other(
            "RUSHORT_API_KEY must contain 32..256 printable ASCII characters",
        ));
    }
    if !ephemeral && cfg.write_token.is_none() {
        return Err(io::Error::other(
            "durable mode requires RUSHORT_API_KEY; use --ephemeral for local benchmarks",
        ));
    }
    if !addr.ip().is_loopback() && (cfg.write_token.is_none() || public_base.is_none()) {
        return Err(io::Error::other(
            "non-loopback bind requires RUSHORT_API_KEY and --public-base",
        ));
    }
    cfg.public_base = public_base
        .unwrap_or_else(|| format!("http://{addr}"))
        .trim_end_matches('/')
        .to_owned();
    validate_url(&cfg.public_base).map_err(io::Error::other)?;
    let base = url::Url::parse(&cfg.public_base).map_err(io::Error::other)?;
    if base.query().is_some() || base.fragment().is_some() || base.path() != "/" {
        return Err(io::Error::other(
            "--public-base must be an origin without path, query or fragment",
        ));
    }
    let store = Arc::new(if ephemeral {
        Store::memory(shards, max_urls)
    } else {
        Store::open(Path::new(&database), shards, max_urls)?
    });
    // Run startup loading before accepting traffic. Never preload into an existing DB.
    if preload > 0 && !store.is_empty() {
        return Err(io::Error::other("--preload requires an empty store"));
    }
    for i in 0..preload {
        store.shorten(&format!("https://example.com/seed/{i}"))?;
    }
    let listener = TcpListener::bind(addr).await?;
    eprintln!(
        "rushort listening on http://{addr}; storage={}; urls={}; max-connections={}",
        if ephemeral {
            "ephemeral"
        } else {
            "sqlite FULL"
        },
        store.len(),
        cfg.max_connections
    );
    serve_with_shutdown(listener, store, cfg, shutdown()).await
}
fn parse<T: std::str::FromStr>(value: &str) -> io::Result<T> {
    value
        .parse()
        .map_err(|_| io::Error::other("invalid numeric argument"))
}
async fn shutdown() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler");
        tokio::select! { _=tokio::signal::ctrl_c()=>{}, _=term.recv()=>{} }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
    eprintln!("rushort: stopping accepts; draining connections for up to 10s");
}
