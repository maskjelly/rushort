# Single-node deployment

Supported topology: HTTPS client → Caddy → loopback HTTP/1.1 shortener → local SQLite + RAM read cache. This is a single-writer service. It does not implement replication or multi-node failover.

1. Build with `cargo build --release --locked`; install `target/release/shortener` as `/usr/local/bin/shortener` on the same OS/architecture.
2. Install `rushort.service` in `/etc/systemd/system/`. Create `/etc/rushort.env` readable only by root, with `RUSHORT_API_KEY` (a random key of at least 32 characters) and `RUSHORT_PUBLIC_BASE=https://s.your-domain.example`. The unit uses a private state directory and listens on loopback.
3. Install Caddy and set `RUSHORT_DOMAIN` in **Caddy's** environment. Use the supplied Caddyfile and run `caddy validate --config /etc/caddy/Caddyfile` before reloading. Allow incoming 80/443, keep 8080 private. Configure DNS for the domain. To serve under a path prefix instead of a dedicated domain, strip it at the proxy (`handle_path /rushort/* { reverse_proxy 127.0.0.1:8080 }`) and build public links from response `code` fields, since `--public-base` must be a bare origin.
4. Start the service with systemd. Verify a keyed POST, GET and HEAD redirect through the actual HTTPS domain. Restart it and verify the same code again before admitting users.

Write and stats requests use `Authorization: Bearer <RUSHORT_API_KEY>`. Keep this credential server-side. Redirects are public; sequential short codes are identifiers, not access-control secrets. This service is intended for trusted link creators. Anonymous shortening requires a separate abuse-control policy.

`GET /api/metrics` is intentionally public and CORS-open (`Access-Control-Allow-Origin: *`) so dashboards can poll it cross-origin. It exposes only counters — never URLs. Sample it once a second and difference the cumulative fields to derive RPS, redirects/s and fails/s.

The backend accepts Content-Length bodies up to 8 KiB and rejects Transfer-Encoding, duplicate Content-Length and malformed HTTP. Use clients that send a length for POST bodies. Caddy handles public TLS and HTTP/2/3; the application protocol is deliberately restricted to HTTP/1.x origin-form requests. Reverse-proxy retry of POST is disabled by default: a client timeout can occur after commit, and retrying can create another code.

SQLite uses WAL, synchronous=FULL and fullfsync=ON. Acknowledgment follows commit and cache publication. The database must be on local storage. Use the configured path consistently; do not bypass its process lock through aliases or edit the database while the service runs. Loading an existing database checks contiguous IDs, valid URLs and the configured capacity.

## Operations

- `/health` and `/ready` become available only after database loading and listener startup. They report that the process serves requests, not that disk capacity is unlimited. `/api/stats` is protected and exposes URL count and capacity.
- Monitor process memory, CPU, disk free space, response latency, 5xx rates and rejected connections at the reverse proxy. Writes return 503 when the storage limit, admission limit or database failure prevents a write.
- Default process limits are 1,024 connections, 32 concurrent admitted writes, a five-second request/header/body deadline and five-second socket-write deadline. The unit caps URLs at 200,000 and memory at 1 GiB; adjust both together for URL lengths and traffic. The standalone binary defaults to 1 million URLs.
- SIGINT/SIGTERM stop accepting new connections and allow up to ten seconds for current connections. A database write already running on a blocking worker finishes rather than being cancelled mid-commit.
- Rotate the write key through the environment file and restart the service. There is no overlapping-key rotation window.
- For a consistent live backup, use SQLite's backup API or the SQLite CLI `.backup` command to a separate path; do not copy only the main file while WAL writes are active. Keep backups off-host and test restoring one to an isolated instance. For a cold backup, stop the service first.
- There is no retention or deletion API. Capacity is deliberately bounded; plan archival or a larger capacity before reaching it. Storage is append-only and duplicate URLs receive new codes.

Caddy and systemd templates require validation on the deployment host. Local macOS testing does not establish public HTTPS throughput, Linux service configuration or power-loss durability. Process-kill recovery is covered by the automated test; a power-loss test and long production soak have not been performed.

References: [Caddy reverse proxy](https://caddyserver.com/docs/caddyfile/directives/reverse_proxy), [Caddy request limits](https://caddyserver.com/docs/caddyfile/directives/request_body), [SQLite durability settings](https://www.sqlite.org/pragma.html#pragma_synchronous).
