# Go configurations
GO ?= go
SERVER_DIR = ./server
GENERATOR_DIR = ./generator

# Binary names
SERVER_BIN = server
GENERATOR_BIN = generator

# Cross-platform settings
ifeq ($(OS),Windows_NT)
	SERVER_BIN := $(SERVER_BIN).exe
	GENERATOR_BIN := $(GENERATOR_BIN).exe
	RM = -del /q /f
else
	RM = rm -f
endif

.PHONY: all build test bench run-server run-generator clean

all: build

build:
	$(GO) build -o $(SERVER_BIN) $(SERVER_DIR)
	$(GO) build -o $(GENERATOR_BIN) $(GENERATOR_DIR)

test:
	$(GO) test -v -cover -coverprofile="coverage.out" ./...
	$(GO) tool cover -func="coverage.out"

bench:
	$(GO) test -bench="." -benchmem ./...

run-server: build
	./$(SERVER_BIN)

run-generator: build
	./$(GENERATOR_BIN)

clean:
	$(RM) $(SERVER_BIN) $(GENERATOR_BIN) coverage.out
