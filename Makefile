SHELL := /bin/bash
COMPOSE := docker compose -f deploy/docker-compose.yml
GO ?= go
SERVICES := gate queue admission scorer shop

.DEFAULT_GOAL := help

.PHONY: help
help: ## 사용 가능한 타깃 목록
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-16s\033[0m %s\n", $$1, $$2}'

## ---------- 개발 스택 ----------

.PHONY: dev
dev: ## docker compose로 Redis/Kafka/PG + 전 서비스 기동
	$(COMPOSE) up -d --build
	@echo ""
	@echo "  gate       http://localhost:8080"
	@echo "  queue      http://localhost:8081"
	@echo "  admission  http://localhost:8082"
	@echo "  scorer     http://localhost:8083"
	@echo "  shop       http://localhost:8084"
	@echo "  대기실 UI  http://localhost:8088"
	@echo "  prometheus http://localhost:9090"
	@echo "  grafana    http://localhost:3000"
	@echo ""
	@bash scripts/check-exposure.sh || \
		echo "  ↑ 포트가 외부에 열려 있다. 'make check-exposure' 로 다시 확인할 것."

.PHONY: dev-infra
dev-infra: ## 인프라(Redis/Kafka/PG)만 기동 — 서비스는 로컬에서 go run
	$(COMPOSE) up -d redis kafka postgres

.PHONY: down
down: ## 스택 종료 (볼륨 유지)
	$(COMPOSE) down

.PHONY: clean
clean: ## 스택 종료 + 볼륨 삭제
	$(COMPOSE) down -v

.PHONY: logs
logs: ## 전 서비스 로그 팔로우
	$(COMPOSE) logs -f --tail=100

.PHONY: ps
ps: ## 컨테이너 상태
	$(COMPOSE) ps

## ---------- 빌드 / 검증 ----------

.PHONY: build
build: ## 전 서비스 바이너리 빌드
	@mkdir -p bin
	@for s in $(SERVICES); do \
		echo "building $$s"; \
		$(GO) build -trimpath -o bin/$$s ./cmd/$$s || exit 1; \
	done

.PHONY: test
test: ## 전체 단위 테스트
	$(GO) test ./...

.PHONY: test-int
test-int: ## testcontainers 통합 테스트 (Docker 필요)
	$(GO) test -tags=integration -timeout=15m ./...

.PHONY: race
race: ## 레이스 디텍터 포함 테스트
	$(GO) test -race ./...

.PHONY: cover
cover: ## 커버리지 리포트 생성
	$(GO) test -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | tail -1

.PHONY: check-exposure
check-exposure: ## 포트 노출 점검 (0.0.0.0 바인딩 탐지)
	@bash scripts/check-exposure.sh

.PHONY: lint
lint: ## golangci-lint 실행
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "golangci-lint 가 필요합니다: brew install golangci-lint"; exit 1; }
	golangci-lint run

.PHONY: fmt
fmt: ## gofmt + go mod tidy
	$(GO) fmt ./...
	$(GO) mod tidy

.PHONY: vet
vet: ## go vet
	$(GO) vet ./...

## ---------- 성능 / 부하 ----------

.PHONY: bench-queue
bench-queue: ## enqueue/position 벤치마크 (실제 Redis 필요)
	$(GO) test -tags=integration -run='^$$' -benchmem -benchtime=2s \
		-bench='^Benchmark(Enqueue|Position|Heartbeat)$$' ./internal/queue/...
	$(GO) test -tags=integration -run='^$$' -benchmem -benchtime=1x \
		-bench='^BenchmarkEnqueue100k$$' ./internal/queue/...

.PHONY: bench-pow
bench-pow: ## PoW 해시율 벤치마크 (봇 비용 계산의 입력 — loadtest/tools/powcost.py)
	$(GO) test -run='^$$' -benchtime=3s -bench='^Benchmark(Digest|Solve)$$' ./internal/challenge/...

.PHONY: loadtest
loadtest: ## k6 혼합 시나리오
	@command -v k6 >/dev/null 2>&1 || { echo "k6 가 필요합니다: brew install k6"; exit 1; }
	k6 run loadtest/k6/mixed.js

.PHONY: loadtest-normal
loadtest-normal: ## k6 정상 사용자 시나리오만
	k6 run loadtest/k6/normal_users.js

.PHONY: loadtest-bots
loadtest-bots: ## k6 봇팜 시나리오만
	k6 run loadtest/k6/bot_farm.js

## ---------- 유틸 ----------

.PHONY: redis-cli
redis-cli: ## 개발 Redis 접속
	$(COMPOSE) exec redis redis-cli

.PHONY: psql
psql: ## 개발 PostgreSQL 접속
	$(COMPOSE) exec postgres psql -U shardgate -d shardgate

.PHONY: secrets
secrets: ## 로컬 .env 용 랜덤 시크릿 생성
	@echo "SG_EVENT_SALT=$$(openssl rand -hex 32)"
	@echo "SG_TOKEN_SIGNING_KEY=$$(openssl rand -hex 32)"
	@echo "SG_CHALLENGE_HMAC_KEY=$$(openssl rand -hex 32)"
