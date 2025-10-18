#!/bin/bash

mkdir -p ./bin
go build -o ./bin/ingest store/main/main.go
go build -o ./bin/tileserver tileserver/server.go
go build -o ./bin/merger merger/main/main.go
go build -o ./bin/meta metadatadb/metadatadb.go
