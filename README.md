# rushort

A minimal, high-throughput URL shortener in Rust with **no web framework** — raw HTTP/1.1 on tokio, a flat slot-array store, and batched pipelined responses.

Measured on a 14-core Apple Silicon Mac (client and server co-resident): **19.2M req/s peak**, **10M req/s sustained open-loop for 10s with zero errors and zero mismatches**.

```
$ curl -s -X POST -H 'content-type: application/json' \
    -d '{"url":"https://example.com/hello"}' http://127.0.0.1:8080/api/shorten
{"code":"0","long_url":"https://example.com/hello","short_url":"http://127.0.0.1:8080/0"}

$ curl -i http://127.0.0.1:8080/0
HTTP/1.1 302 Found
location: https://example.com/hello
content-length: 0
```

## Design

- **Raw HTTP/1.1** (`src/lib.rs`): custom allocation-free header parser, span-based (no string copies), one socket task per connection. Handles keep-alive, pipelining, `Content-Length`, `Expect: 100-continue`, `Connection: close`.
- **Batched writes**: every read drains all complete requests currently buffered and replies with a single `write` (up to 256 requests per batch). This is the single most important optimization — it removes per-request syscall/wakeup costs.
- **Store**: codes are base62-encoded ids; a GET decodes the id and indexes directly into a 64-way sharded `Vec<Arc<str>>` — no hashing, no key comparison. Writes are lock-free on the read path.
- **No frameworks**: dependencies are `tokio`, `parking_lot`, `serde_json` (the last only to parse POST bodies and build JSON replies).

Endpoints:

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/shorten` | `{"url":"https://..."}` → 201 with `code`, `short_url`, `long_url` |
| `GET` | `/{code}` | 302 `Location: <original url>` or 404 |
| `GET` | `/health` | `ok` |
| `GET` | `/api/stats` | `{"urls":N,"shards":64}` |

## Run it

```console
$ cargo run --release --bin shortener            # 127.0.0.1:8080
$ cargo run --release --bin shortener -- --bind 0.0.0.0:8080 --preload 100000
```

Options: `--bind ADDR`, `--shards N` (power of two, default 64), `--preload N`.

## Benchmarks

`./bench.sh` reproduces the suite (builds release, starts a server on 127.0.0.1:8080, runs `ab` + the bundled simulator). Published numbers from the final build:

**Hardware**: Apple Silicon, 14 cores, macOS (loopback). Load generator runs on the same machine, so these are lower bounds on server capacity. Release profile: `lto=thin`, `codegen-units=1`.

| Benchmark | Result | Errors |
|---|---|---|
| Round-trip, `ab -k` (c=64) | **162.7k req/s** | 0 |
| Round-trip mixed 95% GET / 5% POST, 64 conns (saturate) | **181.9k req/s**, p99 0.50 ms | 0 |
| Pipelined open-loop, 500k req/s target, 10s | **500,274 req/s** | 0 |
| Pipelined open-loop, 1M req/s target, 10s | **1,000,362 req/s** | 0 |
| Pipelined open-loop, 10M req/s target, 10s (100M requests) | **9,999,152 req/s** | 0 |
| Pipelined saturate (peak) | **19,234,246 req/s** (192M requests) | 0 |

Every redirect response is verified against the URL it should point to; all runs report zero mismatches. At 10M req/s the server used roughly 3-5 of 14 cores; the co-resident generator used 4-6.5.

**Read this before quoting the numbers:**

- The 10M+ figures use **deep pipelining** (128 requests in flight per connection) — a synthetic processing benchmark that amortizes TCP round-trip cost. It measures server request-processing capacity, not what a browser sees.
- Real round-trip traffic (one request per round trip, no pipelining) caps around 160-215k req/s here, limited by macOS loopback/kqueue, not the application (no server thread was above ~46% CPU).
- No NIC is involved. On Linux, expect higher round-trip throughput; reaching 1M+ with real round trips means io_uring (`monoio`/`compio`) or kernel bypass (AF_XDP/DPDK) with many cores and 100GbE NICs.
- A URL shortener in production needs latency and tail behavior, not a single-node headline; these results are a controlled microbenchmark.

Previous iterations for reference: an axum/hyper implementation peaked ~169k req/s on the same hardware; the raw implementation with batched writes is ~30-100x faster on the pipelined path.

## Load simulator

`src/bin/loadgen.rs` is a standalone HTTP/1.1 load generator with:

- **Open-loop rate mode** (`--rps N --duration S`): schedules exactly `rps * duration` requests on absolute deadlines (no drift, no closed-loop lies), fails unless it achieves ≥99% of target with zero errors.
- **Saturate mode** (`--mode saturate`): closed-loop max throughput.
- **Pipelining** (`--pipeline N`): batches of N requests per connection to measure processing capacity beyond TCP round-trip cost.
- Verification: every 302 is checked against the expected long URL; writes are optionally re-verified after the run.
- Latency percentiles per request class, send-lag tracking, reconnect handling.

```console
$ ./target/release/loadgen --rps 10000000 --duration 10 --connections 32 --pipeline 128
```

## Tests

```console
$ cargo test --release
```

11 tests: base62 round-trips, cross-thread code uniqueness, correct resolution per shard, URL validation, and TCP-level integration tests (keep-alive, pipelining, 400/404 paths, concurrent clients verifying redirects).

## Notes / non-goals

- Storage is in-memory and unbounded; restarting loses data. There is no persistence, no dedup of long URLs, and no auth.
- `--shards` should stay a power of two.

## What it would take to hit 10M with real (non-pipelined) traffic

Documented reference points: Seastar ~7M req/s (DPDK, 2×14-core), F-Stack nginx 5M req/s, io_uring 7.8M sustained / 10.04M burst on a 64-core EPYC, dperf 10M+ HTTP connections/s. In Rust the ladder is roughly: io_uring runtimes (monoio/compio) for 1-3M on 16-64 cores, then AF_XDP (`xsk-rs`, `aya`) or DPDK-scale bypass for 5-10M, with 2×100GbE and multiple dedicated load-generator machines. macOS cannot run either.
