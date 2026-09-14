# Contributing

- Keep it minimal. Prod path is `src/lib.rs` + `src/store.rs` + `src/bin/shortener.rs`. `loadgen` is bench-only.
- Before PR, all green:
  ```console
  cargo fmt --check
  cargo clippy --all-targets --locked -- -D warnings
  cargo test --release --locked
  python3 tests/blackbox.py
  ```
- Bench claims need a `target/benchmarks/<stamp>/CLAIMS.md` from `./bench.sh`. Quote the table, not just the peak. Pipelined numbers must say pipeline depth + p99 + drops.
- No new deps without justification. No unsafe without a soundness note.
- Don't commit `target/`, `*.db*`, `.env`, logs, or `__pycache__/`.
