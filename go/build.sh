#!/bin/bash

mkdir -p ./bin
go build -o ./bin/tileserver ./tileserver/server.go
go build -o ./bin/import ./plan/
go build -o ./bin/ingest ./store/main/
go build -o ./bin/merge ./merger/main/
