#!/bin/bash
set -e

podman build -f Dockerfile --target linux -t wplace:linux
podman build -f Dockerfile --target windows -t wplace:windows
podman build -f Dockerfile --target tileserver -t wplace:tileserver