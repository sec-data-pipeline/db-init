FROM --platform=linux/amd64 golang:1.21

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY main.go ./

COPY storage ./storage

COPY request ./request

RUN go build -o ./bin/db-init

CMD ["/app/bin/db-init"]

