# September 30 VPS comparison

Tested a borrowed redirect lookup that avoids Arc reference-count changes by building the redirect response while holding the shard read guard. It did not produce a consistent improvement and was removed. The redirect path remains unchanged.

Linux 5.15, four shared VPS vCPUs. Server pinned to CPUs 0–1; verifying load generator pinned to 2–3. Three alternating baseline/candidate repetitions per workload, eight seconds each. Synthetic traffic was paused and restored in a finally block. The live service stayed running. Both binaries used isolated ephemeral state and port 18080. The baseline was commit ec01ce7; the candidate also contained the monitoring changes now retained in source.

| Workload | Baseline median RPS | Candidate median RPS | Change |
| --- | ---: | ---: | ---: |
| roundtrip | 34,423 | 34,566 | +0.42% |
| pipeline128 | 1,045,055 | 1,028,081 | -1.62% |
| pipeline512 | 1,341,131 | 1,333,296 | -0.58% |

Roundtrip: 64 connections, pipeline 1, 10,000 URLs. Pipeline128: 32 connections, 10,000 URLs. Pipeline512: 32 connections, 1,000 URLs. All 18 completed runs passed with zero drops, transport errors, unexpected statuses or redirect mismatches. Pipelined latency is complete batch latency, not per-request browser latency. A fourth 100,000-URL workload exceeded the setup timeout and is excluded.

These are loopback measurements with a client sharing the server, not public HTTPS capacity. The retained improvements cache `/api/host` for one second, move its system inspection off Tokio workers, and expose monotonic `uptime_ms` for accurate counter sampling. No redirect throughput improvement is claimed.

[Raw measurements](benchmarks/rove-2026-09-30.json).
