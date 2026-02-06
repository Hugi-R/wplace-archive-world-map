#!/bin/bash
set -e

cargo build --release
~/.cargo/bin/wasm-pack build --target web --no-default-features --features wasm
cp pkg/wplacearchive_bg.wasm ../tmp/assets/
cp pkg/wplacearchive.js ../tmp/assets/
cp ../tile-worker.js ../tmp/assets/
cp ../index.html ../tmp/index.html.tmpl