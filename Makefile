# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

# ── Compose file sets ─────────────────────────────────────────────────────────
DC_BASE := docker compose -f docker-compose.yml -f docker-compose.clickhouse.yml
DC_AUTH := docker compose -f docker-compose.yml -f docker-compose.auth.yml -f docker-compose.clickhouse.yml
DC_OTEL := docker compose -f docker-compose.yml -f docker-compose.jaeger.yml -f docker-compose.clickhouse.yml

# ── Helpers ───────────────────────────────────────────────────────────────────

.DEFAULT_GOAL := help
.PHONY: help install lock docker-check env-check env-check-auth gen-cookie-secret gen-batch-secret \
        dev dev-api dev-batch dev-ui \
        test test-ui lint \
        build up down logs ps restart shell-api shell-batch \
        up-auth down-auth logs-auth \
        up-otel down-otel logs-otel \
        clean

# Auto-generate help from ## comments
help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; \
		     /^##/ { printf "\n\033[90m%s\033[0m\n", substr($$0,3) } \
		     !/^##/ { printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2 }'

# ── Setup ─────────────────────────────────────────────────────────────────────
## Setup

install: ## Install Python and Node dependencies
	poetry install
	npm install --prefix ui

lock: ## Regenerate poetry.lock after changing pyproject.toml
	poetry lock

docker-check: ## Verify the Docker daemon is reachable
	@docker info > /dev/null 2>&1 || { \
		echo ""; \
		echo "Error: Docker daemon is not running."; \
		echo ""; \
		echo "  macOS (Colima):          colima start"; \
		echo "  macOS (Docker Desktop):  open -a Docker"; \
		echo "  Linux:                   sudo systemctl start docker"; \
		echo ""; \
		exit 1; \
	}

env-check: ## Verify .env exists and VZ_MODELS_DIR is set
	@test -f .env || { echo "Error: .env not found — run: cp .env.example .env"; exit 1; }
	@. ./.env && test -n "$$VZ_MODELS_DIR" || { echo "Error: VZ_MODELS_DIR not set in .env"; exit 1; }
	@echo "✓ Environment looks good"

env-check-auth: env-check ## Verify OIDC variables are set (required for up-auth)
	@. ./.env && test -n "$$OIDC_ISSUER_URL"    || { echo "Error: OIDC_ISSUER_URL not set";    exit 1; }
	@. ./.env && test -n "$$OIDC_CLIENT_ID"      || { echo "Error: OIDC_CLIENT_ID not set";      exit 1; }
	@. ./.env && test -n "$$OIDC_CLIENT_SECRET"  || { echo "Error: OIDC_CLIENT_SECRET not set";  exit 1; }
	@. ./.env && test -n "$$OAUTH2_COOKIE_SECRET" || { echo "Error: OAUTH2_COOKIE_SECRET not set (run: make gen-cookie-secret)"; exit 1; }
	@echo "✓ OIDC environment looks good"

gen-cookie-secret: ## Generate a value for OAUTH2_COOKIE_SECRET
	@python -c "import secrets, base64; print(base64.b64encode(secrets.token_bytes(32)).decode())"

gen-batch-secret: ## Generate a value for BATCH_SERVICE_SECRET
	@python -c "import secrets; print(secrets.token_hex(32))"

# ── Local development (no containers) ─────────────────────────────────────────
## Local development

dev: ## Start API + batch service + UI locally with hot-reload (requires a running ClickHouse instance)
	@echo ""
	@echo "  API:    http://localhost:8000/docs"
	@echo "  Batch:  http://localhost:8001/docs"
	@echo "  UI:     http://localhost:5173"
	@echo ""
	@echo "  Requires ClickHouse — run 'make clickhouse' first if not already running."
	@echo ""
	@trap 'kill 0' EXIT; \
	  poetry run watchfiles --filter python "poetry run uvicorn api.main:app --port 8000" api core engine semantic tools batch & \
	  poetry run watchfiles --filter python "poetry run uvicorn batch_service.main:app --port 8001" batch_service batch core engine semantic tools & \
	  npm run dev --prefix ui & \
	  wait

dev-api: ## Start API server only
	poetry run watchfiles --filter python "poetry run uvicorn api.main:app --port 8000" api core engine semantic tools batch

dev-batch: ## Start batch service only
	poetry run watchfiles --filter python "poetry run uvicorn batch_service.main:app --port 8001" batch_service batch core engine semantic tools

dev-ui: ## Start UI dev server only
	npm run dev --prefix ui

# ── Testing & linting ─────────────────────────────────────────────────────────
## Testing & linting

test: ## Run Python test suite
	poetry run pytest tests/ -q

test-cov: ## Run Python tests with coverage report
	poetry run pytest tests/ --cov=. --cov-report=term-missing -q

test-ui: ## Run UI tests (vitest)
	npm run test --prefix ui

lint: ## Lint Python (ruff) and TypeScript (eslint)
	poetry run ruff check .
	npm run lint --prefix ui

lint-fix: ## Auto-fix lint errors where possible
	poetry run ruff check . --fix
	npm run lint --prefix ui -- --fix

# ── Docker — no auth ──────────────────────────────────────────────────────────
## Docker — no auth
## Serves on http://localhost — no login required (uses DEV_USER bypass).

build: docker-check ## Build all Docker images
	$(DC_BASE) build

up: docker-check env-check ## Build and start containers (no auth)
	$(DC_BASE) up --build -d
	@echo ""
	@echo "  UI:                http://localhost"
	@echo "  API docs:          http://localhost/api/docs"
	@echo "  Traefik dashboard: http://localhost:8080"
	@echo "  Connect to CH:     docker exec -it vizgrams-clickhouse-1 clickhouse-client"
	@echo ""
	@echo "Run 'make logs' to follow output, 'make down' to stop."

down: docker-check ## Stop and remove containers
	$(DC_BASE) down

logs: docker-check ## Follow logs from all containers
	$(DC_BASE) logs -f

ps: docker-check ## Show container status
	$(DC_BASE) ps

restart: docker-check ## Restart all containers
	$(DC_BASE) restart

shell-api: docker-check ## Open a shell in the running API container
	$(DC_BASE) exec api sh

shell-batch: docker-check ## Open a shell in the running batch container
	$(DC_BASE) exec batch sh

# ── Docker — with OIDC auth ───────────────────────────────────────────────────
## Docker — with OIDC auth
## Full auth stack using any OIDC provider (Auth0, Google, Entra ID, etc.).
## Set OIDC_ISSUER_URL, OIDC_CLIENT_ID, OIDC_CLIENT_SECRET, and
## OAUTH2_COOKIE_SECRET in .env before running.

up-auth: docker-check env-check-auth ## Build and start full stack with OIDC auth
	$(DC_AUTH) up --build -d
	@echo ""
	@echo "  UI (login required): http://localhost"
	@echo "  Traefik dashboard:   http://localhost:8080"
	@echo ""
	@echo "Run 'make logs-auth' to follow output, 'make down-auth' to stop."

down-auth: ## Stop and remove auth stack
	$(DC_AUTH) down

logs-auth: ## Follow logs from auth stack
	$(DC_AUTH) logs -f

# ── Docker — with Jaeger tracing ──────────────────────────────────────────────
## Docker — with Jaeger tracing
## Adds a Jaeger all-in-one container; both API and batch export spans to it.
## Jaeger UI: http://localhost:16686

up-otel: docker-check env-check ## Build and start containers with Jaeger tracing
	$(DC_OTEL) up --build -d
	@echo ""
	@echo "  UI:               http://localhost"
	@echo "  Jaeger UI:        http://localhost:16686"
	@echo "  API docs:         http://localhost/api/docs"
	@echo ""
	@echo "Run 'make logs-otel' to follow output, 'make down-otel' to stop."

down-otel: docker-check ## Stop otel stack
	$(DC_OTEL) down

logs-otel: docker-check ## Follow logs from otel stack
	$(DC_OTEL) logs -f

# ── Cleanup ───────────────────────────────────────────────────────────────────
## Cleanup

clean: docker-check ## Remove containers, dangling images, and Python caches
	-$(DC_BASE) down --remove-orphans 2>/dev/null
	-$(DC_AUTH) down --remove-orphans 2>/dev/null
	docker image prune -f
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	@echo "✓ Clean"
