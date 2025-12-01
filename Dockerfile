FROM docker.io/golang:1.24 AS builder

RUN apt-get update && apt-get install -y gcc-multilib gcc-mingw-w64

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY ./img img
COPY ./tileserver tileserverSrc
RUN go build -o tileserver ./tileserverSrc/server.go
RUN GOOS=windows GOARCH=amd64 CGO_ENABLED=1 CXX=x86_64-w64-mingw32-g++ CC=x86_64-w64-mingw32-gcc go build -o tileserver.exe tileserverSrc/server.go
COPY ./store store
COPY ./merger merger
COPY ./plan plan
RUN go build -o import ./plan/
RUN GOOS=windows GOARCH=amd64 CGO_ENABLED=1 CXX=x86_64-w64-mingw32-g++ CC=x86_64-w64-mingw32-gcc go build -o import.exe ./plan/

FROM scratch AS windows
COPY --from=builder /app/import.exe /app/tileserver.exe /

FROM scratch AS linux
COPY --from=builder /app/import /app/tileserver /

FROM gcr.io/distroless/base-debian12 as tileserver
COPY --chmod=0755 --from=builder /app/tileserver /
ENV PORT=8080
ENV DATA_PATH=/data
EXPOSE 8080
ENTRYPOINT ["/tileserver"]
