# rushort

[![Checks](https://github.com/maskjelly/rushort/actions/workflows/ci.yml/badge.svg)](https://github.com/maskjelly/rushort/actions/workflows/ci.yml)
![License: MIT](https://img.shields.io/badge/license-MIT-green)

Minimal Rust URL shortener. Raw HTTP/1.1 on tokio. No web framework.

- POST URL → get code → GET code redirects (302).
- SQLite WAL (FULL + fullfsync) + RAM read cache. Kill-safe. Restart keeps data.
- Auth writes with `RUSHORT_API_KEY` (≥32 chars). Reads public.
- Limits: 1024 conns, 32 concurrent writes, 5s timeouts, 8KB body, 16KB head, 1M URLs default.
- Prod server ≈900 lines (`src/lib.rs` + `src/store.rs` + `src/bin/shortener.rs`). Loadgen is a separate bench binary.

Live demo: [whiteye.in/telemetry](https://whiteye.in/telemetry) — traffic charts with 1/2/5-minute windows, response counters, host resources, pause/resume and CSV export, plus a form that shortens URLs into `whiteye.in/s/…` links.

```
$ cargo run --release --bin shortener -- --ephemeral &
$ curl -s -X POST -H 'content-type: application/json' \
    -d '{"url":"https://example.com/hello"}' http://127.0.0.1:8080/api/shorten
{"code":"0","long_url":"https://example.com/hello","short_url":"http://127.0.0.1:8080/0"}
$ curl -i http://127.0.0.1:8080/0
HTTP/1.1 302 Found
location: https://example.com/hello
```

## Self-run (60s)

```console
$ cargo test --release --locked          # 26 Rust tests
$ python3 tests/blackbox.py              # 10 fail-closed + SIGKILL tests
$ ./bench.sh --repeats 1 --seconds 10    # quick full loop, ~60s, writes target/benchmarks/<stamp>/
$ cat target/benchmarks/*/CLAIMS.md      # machine-stamped numbers, quote included
```

Full validation (~2min, numbers quoted below):

```console
$ ./bench.sh --repeats 3 --seconds 30
```

## Run

```console
$ cargo run --release --bin shortener -- --ephemeral                    # RAM-only, local bench
$ RUSHORT_API_KEY=$(openssl rand -hex 32) cargo run --release --bin shortener -- --db urls.db
$ cargo run --release --bin shortener -- --help
```

Flags: `--bind`, `--db`, `--public-base`, `--ephemeral`, `--shards`, `--max-urls`, `--max-connections`, `--max-writes`, `--timeout`, `--preload`.

Endpoints:

| Method | Path | Result |
|---|---|---|
| `POST` | `/api/shorten` `{"url":"https://..."}` + `Authorization: Bearer <key>`* | 201 `code`/`short_url`/`long_url`, 400/401/503 |
| `GET` | `/{code}` | 302 `Location`, or 404 |
| `HEAD` | `/{code}`, `/health` | same headers, no body |
| `GET` | `/health`, `/ready` | 200 `ok` (public) |
| `GET` | `/api/stats` + auth | 200 `urls`/`shards`/`capacity` |
| `GET` | `/api/host` | 200 host facts (public, CORS-open): CPU, memory, OS and load averages. |
| `GET` | `/api/metrics` | 200 counters (public, CORS-open): `uptime_s`, `requests`, `redirects`, `writes`, `errors_4xx`, `errors_5xx`, `urls`, `capacity`. Sample it to derive RPS. |

\*Auth required when `RUSHORT_API_KEY` is set (always required for durable mode + non-loopback binds). Ephemeral loopback bench omits it.

## Measured (M4 Pro, loopback, client+server same box)

`./bench.sh` reproduces. Every redirect verified. Drops/errors fail the run. Raw logs in `target/benchmarks/<stamp>/`.

From `target/benchmarks/validation-isolated` (30s durable + 3×5s saturate + 100M):

- Durable mixed (94% GET / 5% POST / 1% miss): **1,158 RPS, p99 6.39ms, 0 drops** (30s). 2× burst: **2,315 RPS, p99 8.78ms**.
- Round-trip saturate (no pipeline, 64 conns): **median 164,907 RPS, p99 ~0.5ms**.
- Pipeline peak saturate (128/batch, 32 conns): **median 16,281,158 RPS, p99 ~0.5ms**. Best single run: **16,847,534 RPS**.
- Pipeline 100M in 10s (rate 10M/s, 32 conns, pipeline 128, queue 128): **100,000,000 ok in 10.002s, 9,997,937 RPS, p99 17.62ms, 0 drops**.
- Wide mixed (100k seeds, 10% miss): **13,170,223 RPS**.

Single number: **10M RPS**. Largest eye-catch: **16.8M RPS**.

Quote-post:

> My Rust URL shortener processed 100,000,000 randomized redirect GETs in 10.002 seconds on localhost, using 32 connections with pipeline depth 128. In-memory processing benchmark; durable mixed traffic measured separately.

Read before quoting:

- 10M+ uses deep pipelining (128 reqs/batch). Measures processing, not browser latency.
- Real round-trip caps ~165k RPS here (macOS loopback, not app CPU).
- No NIC/TLS. Linux + io_uring / kernel bypass needed for 1M+ real RPS.
- Durable test is a rate check (1,158 RPS = 100M/day avg), not a 24h soak.
- 100M test runs on a fresh server; suite order matters for thermal throttling.

## VPS follow-up (September 2026)

A controlled comparison on the four-vCPU `rove` host measured the unchanged implementation at **951k RPS with pipeline depth 128** (median of three 8-second runs, p99 batch latency 48.2–48.6ms) and **32.8k RPS without pipelining**. Client and server shared the host, pinned to separate CPU pairs; all runs had zero drops, transport errors, or redirect mismatches. These measurements are not comparable to the M4 Pro results above or public HTTPS capacity.

Batching metrics updates and reducing redirect-cache contention did not establish a consistent improvement, so neither experiment was deployed. See the [method and results](docs/performance-2026-09-14.md) and [raw measurements](docs/benchmarks/rove-2026-09-14.json).

The live demo includes a continuous synthetic load generator. Dashboard traffic is not organic visitor traffic; chart history is collected only while the browser is open. The public website redirect path also adds an upstream network request, so user-visible latency needs separate measurement from Rust throughput.

## Loadgen

- `--mode rate`: exact `rps×duration` on absolute deadlines. Needs ≥99%, 0 errors/drops.
- `--mode saturate`: max closed-loop.
- `--pipeline N`: N reqs per write. Latency = full batch, never divided.
- `--queue N`: bounded batches/conn. Overload drops and fails. Included in latency.
- Verifies every 302 location + post-run write re-check.

```console
$ ./target/release/loadgen --rps 10000000 --duration 10 --connections 32 --pipeline 128 --queue 128
$ ./target/release/loadgen --mode saturate --duration 5 --connections 64 --seed 1000
```

## Structure

```
src/lib.rs            # HTTP/1.1 parser, routing, conn loop, batched writes
src/store.rs          # sharded RAM cache + SQLite WAL writer (commit-before-ack)
src/bin/shortener.rs  # CLI, config, graceful shutdown
src/bin/loadgen.rs    # rate/saturate loadgen, verifier (bench-only)
tests/integration.rs  # TCP-level: pipeline, limits, auth, HEAD, fragmentation
tests/blackbox.py     # fail-closed fixtures + SIGKILL durability
scripts/simulate.py   # full bench orchestration, writes CLAIMS.md + summary.json
deploy/               # Caddyfile + systemd unit + ops notes (single-node)
```

## Deploy

Single writer. Caddy (TLS) → 127.0.0.1:8080 → local SQLite. See [`deploy/README.md`](deploy/README.md).

- Set `RUSHORT_API_KEY` + `RUSHORT_PUBLIC_BASE`. Keep 8080 private.
- Backup via SQLite `.backup`, not file copy. No replication / multiserver.
- Codes are sequential IDs (base62), not secrets. Trusted creators only; anonymous shortening needs abuse controls.

Behind a path prefix (e.g. serving the demo under `/rushort/`), strip it at the proxy — the app only speaks origin-form paths:

```caddy
handle_path /rushort/* {
    reverse_proxy 127.0.0.1:8080
}
```

Note: `short_url` in POST responses is built from `--public-base`, which must be a bare origin. Under a path prefix, build public links from the returned `code` instead.

## Contributing

- `cargo fmt --check`, `cargo clippy --all-targets --locked -- -D warnings`, `cargo test --release --locked`, `python3 tests/blackbox.py` — all green before PR.
- Keep the hot path allocation-free. Report bench numbers with `target/benchmarks/<stamp>/CLAIMS.md`, not headlines alone.
- See [CONTRIBUTING.md](CONTRIBUTING.md) and [SECURITY.md](SECURITY.md).

## License

MIT — see [LICENSE-MIT](LICENSE-MIT).
