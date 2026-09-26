# Rove VPS benchmark rerun — 2026-09-26

The four-vCPU KVM host that runs rushort is a $4 VPS according to its owner. This rerun sought the highest *verified* redirect throughput without changing the production binary or database. It did **not** measure 16M RPS on the VPS.

## Setup

- Host: `rove`, Linux 5.15.0-139-generic, four KVM vCPUs (2.49 GHz reported).
- Source checkout: `ec01ce79f3e4baeecb7fed4d50f68d51d71fdc11`, clean. Tested `target/release/shortener` SHA-256 `e0bbe0e690b48bf30fb33d3a541c91a7c9ae7eae3128faa1bd05c7fe83500ba5` and `target/release/loadgen` SHA-256 `c20a8ca72eeefa42e2e9c8b2ffabd54e93f3364b5625d453c874bfc21161c456`.
- Separate RAM-only server on `127.0.0.1:18080`, with 1,000 deterministic URLs preloaded, except the first baseline with 10,000. Client and server shared the VPS over HTTP/1.1 loopback. No TLS or network interface was measured.
- The continuous synthetic `rushort-traffic.service` was stopped during each suite and restored afterward. The production `rushort.service` remained running. Both services were active and `/health` returned `ok` after testing.
- The checked-in load generator verified every redirect and rejected drops, transport errors, unexpected statuses, and URL mismatches. RPS is successful responses divided by actual elapsed time. Pipeline p99 is full batch-completion latency.

## Results

| Workload | Result | Verification |
| --- | ---: | --- |
| Best single saturation run, 16 connections, pipeline 256, 5 seconds | **1,465,138 RPS** | Passed; zero drops/errors/mismatches |
| Repeated saturation, 32 connections, pipeline 512, three 8-second runs | **1,418,510 median RPS** | Range 1,383,376–1,428,007; all passed; p99 batch latency 58.1–59.4 ms |
| Fixed 1.3M RPS target, 32 connections, pipeline 512, three 10-second runs | **13,000,000 redirects per run** | All passed; achieved 1,291,244–1,295,931 RPS over 10.03–10.07 seconds; p99 batch latency 116–264 ms |
| Fixed 1.2M RPS target, same configuration, 10 seconds | **12,000,000 redirects** | Passed; achieved 1,199,289 RPS; p99 batch latency 46.9 ms |
| Fixed 1.4M RPS target, same configuration, 10 seconds | **Failed target** | All 14M redirects were correct, but completion took 10.62 seconds: 1,318,548 RPS, under the 99% required rate; p99 904 ms |

The full [raw JSON](benchmarks/rove-2026-09-26.json) contains all 25 runs, including the configuration sweep. The earlier [2026-09-14 VPS measurement](performance-2026-09-14.md) was 951,012 median RPS with pinned CPU pairs, 10,000 seeds, pipeline 128, and Linux 5.4. That setup differs from this unpinned, 1,000-seed, pipeline-512 rerun; the difference is not evidence of a software speedup.

The 16.8M RPS figure in the project README was measured on an Apple M4 Pro under deep pipelining. These VPS tests found no basis for attributing that result to this host. Neither machine's loopback result is public HTTPS capacity.

## Reproduction

Run against an isolated server while the synthetic traffic service is paused, and restore that service afterward:

```sh
target/release/shortener --bind 127.0.0.1:18080 --ephemeral --preload 1000
target/release/loadgen --target http://127.0.0.1:18080 --preloaded \
  --mode saturate --duration 8 --connections 32 --pipeline 512 --seed 1000
target/release/loadgen --target http://127.0.0.1:18080 --preloaded \
  --rps 1300000 --duration 10 --connections 32 --pipeline 512 \
  --queue 128 --seed 1000
```
