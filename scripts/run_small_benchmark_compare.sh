#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -z "${BENCHMARK_LABEL:-}" ]]; then
  export BENCHMARK_LABEL="bench-$(date -u +"%Y%m%dT%H%M%SZ")"
fi

bash scripts/run_small_benchmark.sh fixed
export BENCHMARK_RESET=false
bash scripts/run_small_benchmark.sh autothrottle
