.PHONY: bins clean setup test test-race cover cover_ci cover_profile

SHELL = /bin/bash

PROJECT_ROOT=github.com/uber/cherami-server
export GO15VENDOREXPERIMENT=1
NOVENDOR = $(shell GO15VENDOREXPERIMENT=1 glide novendor)
TEST_ARG ?= -race -v -timeout 5m
TEST_NO_RACE_ARG ?= -timeout 5m
BUILD := ./build

export PATH := $(GOPATH)/bin:$(PATH)

export CHERAMI_STORE=$(shell dirname `mktemp -u store.test.XXX`)/cherami_store
export CHERAMI_CONFIG_DIR=$(CURDIR)/config

# determine whether to use embedded rocksdb
ifneq ($(EMBEDROCKSDB), 0)
    EMBED = -tags=embed
endif

# Automatically gather all srcs
ALL_SRC := $(shell find . -name "*.go" | grep -v -e Godeps -e vendor \
	-e ".*/\..*" \
	-e ".*/_.*" \
	-e ".*/mocks.*")

# all directories with *_test.go files in them
TEST_DIRS := $(sort $(dir $(filter %_test.go,$(ALL_SRC))))

test: bins
	@for dir in $(TEST_DIRS); do \
		go test $(EMBED) "$$dir" $(TEST_NO_RACE_ARG) $(shell glide nv); \
	done;

test-race:
	@for dir in $(TEST_DIRS); do \
		go test $(EMBED) "$$dir" $(TEST_ARG) | tee -a "$$dir"_test.log; \
	done;	       

checkcassandra:
	@if ! which cqlsh | grep -q /; then \
		echo "cqlsh not in PATH. please install cassandra and cqlsh" >&2; \
		exit 1; \
	fi

server_dep:
	glide install

bins: server_dep
	go build -i $(EMBED) -o cherami-server cmd/standalone/main.go
	go build -i $(EMBED) -o chreami-replicator-server cmd/replicator/main.go
	go build -i -o cherami-cli cmd/tools/cli/main.go
	go build -i -o cherami-admin cmd/tools/admin/main.go
	go build -i -o cherami-replicator-tool cmd/tools/replicator/main.go
	go build -i -o cherami-cassandra-tool cmd/tools/cassandra/main.go

cover_profile: clean bins
	@echo Testing packages:
	@for dir in $(TEST_DIRS); do \
		mkdir -p $(BUILD)/"$$dir"; \
		go test $(EMBED) "$$dir" $(TEST_ARG) -coverprofile=$(BUILD)/"$$dir"/coverage.out || exit 1; \
	done

cover: cover_profile
	@for dir in $(TEST_DIRS); do \
		go tool cover -html=$(BUILD)/"$$dir"/coverage.out; \
	done

cover_ci: cover_profile
	@for dir in $(TEST_DIRS); do \
		goveralls -coverprofile=$(BUILD)/"$$dir"/coverage.out -service=travis-ci || echo -e "\x1b[31mCoveralls failed\x1b[m"; \
	done

clean:
	rm -f cherami-server chreami-replicator-server cherami-cli cherami-admin cherami-replicator-tool cherami-cassandra-tool
	rm -Rf vendor/*
