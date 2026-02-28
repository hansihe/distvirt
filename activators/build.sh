#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OUT="$SCRIPT_DIR/target/components"
mkdir -p "$OUT"
for dir in "$SCRIPT_DIR"/*/; do
    [ -f "$dir/Cargo.toml" ] || continue
    name="$(basename "$dir")"
    echo "Building $name..."
    (cd "$dir" && cargo component build --release)
    cp "$dir/target/wasm32-wasip1/release/"*.wasm "$OUT/$name.wasm" 2>/dev/null || \
    cp "$dir/target/wasm32-unknown-unknown/release/"*.wasm "$OUT/$name.wasm" 2>/dev/null || \
    echo "  Warning: no .wasm output found for $name"
done
echo "Components in $OUT:"
ls -la "$OUT"
