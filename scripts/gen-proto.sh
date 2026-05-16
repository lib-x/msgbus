#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v buf >/dev/null 2>&1; then
  echo "buf is required. Install with: go install github.com/bufbuild/buf/cmd/buf@latest" >&2
  exit 1
fi

buf lint
buf generate

