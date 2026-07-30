.SILENT:

APP=bluray-ripper

CMD=./cmd/bluray-ripper

KAFKA_CONTAINER ?= kafka
KAFKA_CMD = docker exec $(KAFKA_CONTAINER) /opt/kafka/bin/kafka-topics.sh
KAFKA_BOOTSTRAP ?= localhost:9092

TOPICS = \
	disc.detected \
	disc.discdata \
	disc.metadata \
	disc.ripped \
	disc.rip.progress \
	disc.converted \
	disc.convert.progress \
	rip.completed \
	transcode.requested \
	transcode.completed

.PHONY: run build test fmt vet clean kafka-list kafka-create kafka-delete

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

kafka-list:
	$(KAFKA_CMD) \
		--bootstrap-server $(KAFKA_BOOTSTRAP) \
		--list

kafka-create:
	@for topic in $(TOPICS); do \
		echo "Creating $$topic"; \
		$(KAFKA_CMD) \
			--bootstrap-server $(KAFKA_BOOTSTRAP) \
			--create \
			--if-not-exists \
			--topic $$topic \
			--partitions 1 \
			--replication-factor 1; \
	done

kafka-delete:
	@echo "Deleting all topics..."
	@$(KAFKA_CMD) \
		--bootstrap-server $(KAFKA_BOOTSTRAP) \
		--list | while read topic; do \
			echo "Deleting $$topic"; \
			$(KAFKA_CMD) \
				--bootstrap-server $(KAFKA_BOOTSTRAP) \
				--delete \
				--topic $$topic; \
	done