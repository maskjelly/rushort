#!/usr/bin/env bash
# Isolated real-HTTP simulation; all raw results retained under target/benchmarks/.
set -euo pipefail
cd "$(dirname "$0")"
cargo build --release --locked
exec python3 scripts/simulate.py "$@"
