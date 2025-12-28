#!/bin/bash
set -e

cargo build --release
~/.cargo/bin/wasm-pack build --target web --no-default-features --features wasm
cp pkg/wplace_archive_world_map_bg.wasm tmp/assets/
cp pkg/wplace_archive_world_map.js tmp/assets/
cp tile-worker.js tmp/assets/