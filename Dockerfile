FROM docker.io/golang:1.24 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o tileserver ./backend/tileserver/server.go
RUN chmod +x tileserver

FROM gcr.io/distroless/base-debian12
COPY --chmod=0755 --from=builder /app/tileserver /
ENV PORT=8080
ENV DATA_PATH=/data
EXPOSE 8080
ENTRYPOINT ["/tileserver"]
