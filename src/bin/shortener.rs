use std::net::SocketAddr;
use std::sync::Arc;

use rushort::{Store, serve};
use tokio::net::TcpListener;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::io::Result<()> {
    let mut bind = "127.0.0.1:8080".to_owned();
    let mut shards = 64usize;
    let mut preload = 0usize;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--bind" => bind = next_val(&mut args, &arg),
            "--shards" => shards = parse_val(&mut args, &arg),
            "--preload" => preload = parse_val(&mut args, &arg),
            "-h" | "--help" => {
                println!(
                    "rushort server\n\n\
                     USAGE: shortener [--bind ADDR] [--shards N] [--preload N]\n\n\
                     OPTIONS:\n  \
                     --bind ADDR     listen address (default 127.0.0.1:8080)\n  \
                     --shards N      storage shards, rounded up to a power of two (default 64)\n  \
                     --preload N     pre-seed N URLs (default 0)\n"
                );
                return Ok(());
            }
            other => {
                eprintln!("shortener: unknown argument `{other}` (try --help)");
                std::process::exit(2);
            }
        }
    }

    let store = Arc::new(Store::with_shards(shards.next_power_of_two()));
    for i in 0..preload {
        store.shorten(&format!("https://example.com/seed/{i}"));
    }

    let addr: SocketAddr = bind
        .parse()
        .unwrap_or_else(|_| panic!("shortener: invalid bind address `{bind}`"));
    let listener = TcpListener::bind(addr).await?;
    eprintln!(
        "rushort listening on http://{addr} (shards={}, preloaded={preload})",
        store.shard_count()
    );

    tokio::select! {
        result = serve(listener, store) => result,
        _ = tokio::signal::ctrl_c() => {
            eprintln!("rushort: shutting down");
            Ok(())
        }
    }
}

fn next_val(args: &mut impl Iterator<Item = String>, flag: &str) -> String {
    args.next().unwrap_or_else(|| {
        eprintln!("shortener: {flag} requires a value");
        std::process::exit(2);
    })
}

fn parse_val<T: std::str::FromStr>(args: &mut impl Iterator<Item = String>, flag: &str) -> T {
    let raw = next_val(args, flag);
    raw.parse().unwrap_or_else(|_| {
        eprintln!("shortener: invalid value `{raw}` for {flag}");
        std::process::exit(2);
    })
}
