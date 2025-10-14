FROM docker.io/golang:1.24 AS builder

RUN apt-get update && apt-get install -y gcc-multilib gcc-mingw-w64

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY ./img img
COPY ./tileserver tileserver
RUN go build -o tileserver ./tileserver/server.go
RUN GOOS=windows GOARCH=amd64 CGO_ENABLED=1 CXX=x86_64-w64-mingw32-g++ CC=x86_64-w64-mingw32-gcc go build -o tileserver.exe tileserver/server.go
COPY ./store store
RUN go build -o ingest store/main/main.go
RUN GOOS=windows GOARCH=amd64 CGO_ENABLED=1 CXX=x86_64-w64-mingw32-g++ CC=x86_64-w64-mingw32-gcc go build -o ingest.exe store/main/main.go
# Merger depends on store
COPY ./merger merger
RUN go build -o merge merger/main/main.go
RUN GOOS=windows GOARCH=amd64 CGO_ENABLED=1 CXX=x86_64-w64-mingw32-g++ CC=x86_64-w64-mingw32-gcc go build -o merge.exe merger/main/main.go
COPY ./metadatadb metadatadb
RUN go build -o meta metadatadb/metadatadb.go
RUN GOOS=windows GOARCH=amd64 CGO_ENABLED=1 CXX=x86_64-w64-mingw32-g++ CC=x86_64-w64-mingw32-gcc go build -o meta.exe metadatadb/metadatadb.go

FROM scratch AS windows
COPY --from=builder /app/merge.exe /app/ingest.exe /app/tileserver.exe /app/meta.exe /

FROM scratch AS linux
COPY --from=builder /app/merge /app/ingest /app/tileserver /app/meta /

FROM gcr.io/distroless/base-debian12
COPY --chmod=0755 --from=builder /app/tileserver /
ENV PORT=8080
ENV DATA_PATH=/data
EXPOSE 8080
ENTRYPOINT ["/tileserver"]
