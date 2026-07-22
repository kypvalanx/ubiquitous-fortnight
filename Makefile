.SILENT:

APP=bluray-ripper

CMD=./cmd/bluray-ripper

.PHONY: run build test fmt vet clean

run:
	go run $(CMD)

event-debug:
	go run ./cmd/event-debugger

build:
	mkdir -p bin
	go build -o bin/$(APP) $(CMD)

test:
	go test ./...

fmt:
	go fmt: ./...

vet:
	go vet./...

clean:
	rm -rf bin

kafka-up:
	docker compose up -d

kafka-down:
	docker compose down

kafka-logs:
	docker compose logs -f kafka

all: fmt test build

help:
	echo "run build test fmt vet clean all"

