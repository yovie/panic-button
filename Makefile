SHELL := /bin/bash

SERVER_PORT ?= 18080
MQTT_PORT ?= 1883

COMPOSE := SERVER_PORT=$(SERVER_PORT) MQTT_PORT=$(MQTT_PORT) docker compose
CARGO := $(shell command -v cargo 2>/dev/null || echo $(HOME)/.cargo/bin/cargo)

.DEFAULT_GOAL := help

.PHONY: help build deploy-dev stop-dev logs ps sim test fmt clippy check clean

help:
	@echo "Panic Button"
	@echo ""
	@echo "  make deploy-dev   Build image lalu jalankan server + mosquitto + simulator"
	@echo "                    Web UI : http://localhost:$(SERVER_PORT)"
	@echo "                    MQTT   : localhost:$(MQTT_PORT) (anonim, publik)"
	@echo "  make stop-dev     Hentikan semua service (volume broker disimpan)"
	@echo "  make clean        Hentikan semua service dan hapus volume"
	@echo "  make logs         Ikuti log semua service"
	@echo "  make ps           Status container"
	@echo "  make sim          Simulator interaktif (p = panic, r = resolve, q = quit)"
	@echo "  make build        Build image Docker"
	@echo "  make test         cargo test"
	@echo "  make clippy       cargo clippy (warning = error)"
	@echo "  make fmt          cargo fmt"
	@echo "  make check        fmt + clippy + test"
	@echo ""
	@echo "Override port: make deploy-dev SERVER_PORT=8080 MQTT_PORT=1883"

build:
	docker compose build

deploy-dev: build
	$(COMPOSE) up -d --remove-orphans --wait mosquitto server
	$(COMPOSE) --profile sim up -d --remove-orphans simulator
	@echo ""
	@echo "Panic Button aktif:"
	@echo "  Web UI      : http://localhost:$(SERVER_PORT)"
	@echo "  MQTT broker : localhost:$(MQTT_PORT) (anonim, publik)"
	@echo "  Demo        : simulator auto-panic tiap 45 detik"
	@echo "  Status      : make ps"
	@echo "  Log         : make logs"

stop-dev:
	$(COMPOSE) --profile sim down

clean:
	$(COMPOSE) --profile sim down -v --remove-orphans

logs:
	$(COMPOSE) --profile sim logs -f --tail=100

ps:
	$(COMPOSE) --profile sim ps

sim:
	$(COMPOSE) --profile sim run --rm -it simulator

test:
	$(CARGO) test

fmt:
	$(CARGO) fmt

clippy:
	$(CARGO) clippy --all-targets -- -D warnings

check: fmt clippy test
