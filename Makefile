SHELL := /bin/bash

SERVER_PORT ?= 18080
MQTT_PORT ?= 1883
DEV_DOMAIN ?= panicbutton.localhost

DEV_URL ?= http://$(DEV_DOMAIN)

STAGING_HOST ?= devserver
STAGING_DIR ?= panic-button-staging
STAGING_PROJECT ?= panic-button-staging
STAGING_PORT ?= 18080
STAGING_MQTT_PORT ?= 1883

COMPOSE := SERVER_PORT=$(SERVER_PORT) MQTT_PORT=$(MQTT_PORT) docker compose
CARGO := $(shell command -v cargo 2>/dev/null || echo $(HOME)/.cargo/bin/cargo)

.DEFAULT_GOAL := help

.PHONY: help build deploy-dev stop-dev deploy-staging stop-staging nginx logs ps sim test fmt clippy check clean

help:
	@echo "Panic Button"
	@echo ""
	@echo "  make deploy-dev   Build image lalu jalankan server + mosquitto + simulator"
	@echo "                    Web UI : $(DEV_URL)"
	@echo "                    MQTT   : $(DEV_DOMAIN):$(MQTT_PORT) (anonim, publik)"
	@echo "  make stop-dev     Hentikan semua service (volume broker disimpan)"
	@echo "  make nginx        Aktifkan vhost nginx $(DEV_URL) (perlu sudo)"
	@echo "  make deploy-staging  Build lokal, kirim source + image ke $(STAGING_HOST), lalu deploy"
	@echo "                    Web UI : http://$(STAGING_HOST):$(STAGING_PORT)"
	@echo "  make stop-staging Hentikan staging di $(STAGING_HOST) (volume disimpan)"
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
	@echo "Override: make deploy-dev SERVER_PORT=8080 MQTT_PORT=1883 DEV_DOMAIN=panicbutton.localhost"

build:
	docker compose build

deploy-dev: build
	$(COMPOSE) up -d --remove-orphans --wait mosquitto server
	$(COMPOSE) --profile sim up -d --remove-orphans simulator
	@echo ""
	@echo "Panic Button aktif:"
	@echo "  Web UI      : $(DEV_URL)"
	@echo "  Panduan     : $(DEV_URL)/panduan.html"
	@echo "  MQTT broker : $(DEV_DOMAIN):$(MQTT_PORT) (anonim, publik)"
	@echo "  Demo        : simulator auto-panic tiap 45 detik"
	@echo "  Status      : make ps"
	@echo "  Log         : make logs"

stop-dev:
	$(COMPOSE) --profile sim down

nginx:
	sudo ln -sfn "$(CURDIR)/nginx/panicbutton.localhost.conf" \
		/etc/nginx/sites-enabled/panicbutton.localhost.conf
	sudo nginx -t
	sudo systemctl reload nginx
	@echo "Vhost aktif: $(DEV_URL)"

deploy-staging: build
	@echo "Sinkronisasi source ke $(STAGING_HOST):$(STAGING_DIR) ..."
	rsync -az --delete \
		--exclude '.git/' \
		--exclude 'target/' \
		--exclude '.env' \
		--exclude 'data/' \
		./ $(STAGING_HOST):$(STAGING_DIR)/
	@echo "Kirim image ke $(STAGING_HOST) ..."
	set -o pipefail; docker save panic-button-server:latest | ssh $(STAGING_HOST) 'docker load'
	ssh $(STAGING_HOST) 'set -e; cd "$(STAGING_DIR)"; COMPOSE_PROJECT_NAME=$(STAGING_PROJECT) SERVER_PORT=$(STAGING_PORT) MQTT_PORT=$(STAGING_MQTT_PORT) docker compose up -d --no-build --remove-orphans --wait mosquitto server'
	@ssh $(STAGING_HOST) 'curl -fsS http://127.0.0.1:$(STAGING_PORT)/health'; echo
	@echo ""
	@echo "Staging aktif di $(STAGING_HOST):"
	@echo "  Web UI      : http://$(STAGING_HOST):$(STAGING_PORT)"
	@echo "  MQTT broker : $(STAGING_HOST):$(STAGING_MQTT_PORT) (anonim, publik)"
	@echo "  Data device : volume Docker (persisten)"
	@echo "  Stop        : make stop-staging"

stop-staging:
	ssh $(STAGING_HOST) 'set -e; cd "$(STAGING_DIR)"; COMPOSE_PROJECT_NAME=$(STAGING_PROJECT) SERVER_PORT=$(STAGING_PORT) MQTT_PORT=$(STAGING_MQTT_PORT) docker compose down'

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
