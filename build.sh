#!/bin/bash

mkdir -p ./bin
go build -o ./bin/tileserver ./tileserver/server.go
go build -o ./bin/import ./plan/
