#!/bin/bash
set -e

podman build -f Dockerfile --target linux -t wplace:linux
podman build -f Dockerfile --target windows -t wplace:windows
podman build -f Dockerfile --target tileserver -t wplace:tileserver

podman tag wplace:tileserver wplace:tileserver-stg
podman save wplace:tileserver-stg > wplace_tileserver_stg.tar