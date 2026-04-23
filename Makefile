# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

# ── Compose file sets ─────────────────────────────────────────────────────────
# ClickHouse is now a first-class dependency included in all stacks.
# Explicit rather than relying on docker-compose.override.yml auto-detection,
# so each target's intent is unambiguous.

DC_BASE         := docker compose -f docker-compose.yml -f docker-compose.clickhouse.yml
DC_AUTH_DEX     := docker compose -f docker-compose.yml -f docker-compose.auth.yml -f docker-compose.dex.yml -f docker-compose.clickhouse.yml
DC_PROD         := docker compose -f docker-compose.yml -f docker-compose.auth.yml -f docker-compose.clickhouse.yml
DC_PROD_DEX     := docker compose -f docker-compose.yml -f docker-compose.auth.yml -f docker-compose.dex.prod.yml -f docker-compose.clickhouse.yml
DC_OTEL         := docker compose -f docker-compose.yml -f docker-compose.jaeger.yml -f docker-compose.clickhouse.yml

# ── Helpers ───────────────────────────────────────────────────────────────────

.DEFAULT_GOAL := help
.PHONY: help install lock docker-check env-check env-check-dex env-check-auth env-check-dex-prod gen-cookie-secret gen-batch-secret gen-dex-secret \
        dev dev-api dev-batch dev-ui \
        test test-ui lint \
        build up down logs ps restart shell-api shell-batch \
        up-auth down-auth \
        up-prod down-prod \
        up-prod-dex down-prod-dex logs-prod-dex \
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

env-check-dex: env-check ## Verify OAUTH2_COOKIE_SECRET is set (required for up-auth with local Dex)
	@. ./.env && test -n "$$OAUTH2_COOKIE_SECRET" || { echo "Error: OAUTH2_COOKIE_SECRET not set (run: make gen-cookie-secret, then add to .env)"; exit 1; }
	@echo "✓ Dex environment looks good"

env-check-auth: env-check ## Verify all OIDC variables are set (required for up-auth and up-prod)
	@. ./.env && test -n "$$OIDC_ISSUER_URL"    || { echo "Error: OIDC_ISSUER_URL not set";    exit 1; }
	@. ./.env && test -n "$$OIDC_CLIENT_ID"      || { echo "Error: OIDC_CLIENT_ID not set";      exit 1; }
	@. ./.env && test -n "$$OIDC_CLIENT_SECRET"  || { echo "Error: OIDC_CLIENT_SECRET not set";  exit 1; }
	@. ./.env && test -n "$$OAUTH2_COOKIE_SECRET" || { echo "Error: OAUTH2_COOKIE_SECRET not set (run: make gen-cookie-secret)"; exit 1; }
	@echo "✓ OIDC environment looks good"

env-check-dex-prod: env-check ## Verify all Dex production variables are set (required for up-prod-dex)
	@. ./.env && test -n "$$DEX_CLIENT_SECRET"    || { echo "Error: DEX_CLIENT_SECRET not set (run: make gen-dex-secret)";       exit 1; }
	@. ./.env && test -n "$$OAUTH2_COOKIE_SECRET" || { echo "Error: OAUTH2_COOKIE_SECRET not set (run: make gen-cookie-secret)"; exit 1; }
	@. ./.env && test -n "$$GOOGLE_CLIENT_ID"     || { echo "Error: GOOGLE_CLIENT_ID not set";     exit 1; }
	@. ./.env && test -n "$$GOOGLE_CLIENT_SECRET" || { echo "Error: GOOGLE_CLIENT_SECRET not set"; exit 1; }
	@. ./.env && test -n "$$APPLE_CLIENT_ID"      || { echo "Error: APPLE_CLIENT_ID not set";      exit 1; }
	@. ./.env && test -n "$$APPLE_TEAM_ID"        || { echo "Error: APPLE_TEAM_ID not set";        exit 1; }
	@. ./.env && test -n "$$APPLE_KEY_ID"         || { echo "Error: APPLE_KEY_ID not set";         exit 1; }
	@. ./.env && test -n "$$APPLE_PRIVATE_KEY"    || { echo "Error: APPLE_PRIVATE_KEY not set";    exit 1; }
	@echo "✓ Dex production environment looks good"

gen-cookie-secret: ## Generate a value for OAUTH2_COOKIE_SECRET
	@python -c "import secrets, base64; print(base64.b64encode(secrets.token_bytes(32)).decode())"

gen-batch-secret: ## Generate a value for BATCH_SERVICE_SECRET
	@python -c "import secrets; print(secrets.token_hex(32))"

gen-dex-secret: ## Generate a value for DEX_CLIENT_SECRET
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

# ── Docker — containers, no auth ──────────────────────────────────────────────
## Docker — containers, no auth
## Use this to test that containers build and run correctly.
## Serves on http://localhost — no login required.

build: docker-check ## Build all Docker images
	$(DC_BASE) build

up: docker-check env-check ## Build and start containers (includes ClickHouse, no auth)
	$(DC_BASE) up --build -d
	@echo ""
	@echo "  UI:               http://localhost"
	@echo "  API docs:         http://localhost/api/docs"
	@echo "  Traefik dashboard: http://localhost:8080"
	@echo "  Connect to CH:    docker exec -it vizgrams-clickhouse-1 clickhouse-client"
	@echo ""
	@echo "Run 'make logs' to follow output, 'make down' to stop."

down: docker-check ## Stop and remove containers (no auth stack)
	$(DC_BASE) down

logs: docker-check ## Follow logs from all containers (no auth stack)
	$(DC_BASE) logs -f

ps: docker-check ## Show container status
	$(DC_BASE) ps

restart: docker-check ## Restart all containers (no auth stack)
	$(DC_BASE) restart

shell-api: docker-check ## Open a shell in the running API container
	$(DC_BASE) exec api sh

shell-batch: docker-check ## Open a shell in the running batch container
	$(DC_BASE) exec batch sh

# ── Docker — full auth stack with local Dex ───────────────────────────────────
## Docker — full auth stack (local Dex OIDC)
## Runs the complete auth flow locally with a built-in identity provider.
## Login at http://localhost with: dev@example.com / password

up-auth: docker-check env-check-dex ## Build and start full stack with local Dex OIDC
	$(DC_AUTH_DEX) up --build -d
	@echo ""
	@echo "  UI (login required): http://localhost"
	@echo "  Credentials:         dev@example.com / password"
	@echo "  Dex console:         http://localhost:5556"
	@echo "  Traefik dashboard:   http://localhost:8080"
	@echo ""
	@echo "Run 'make logs-auth' to follow output, 'make down-auth' to stop."

down-auth: ## Stop and remove full auth stack
	$(DC_AUTH_DEX) down

logs-auth: ## Follow logs from full auth stack
	$(DC_AUTH_DEX) logs -f

# ── Docker — production (real OIDC provider) ──────────────────────────────────
## Docker — production (real OIDC provider)
## Requires OIDC_ISSUER_URL, OIDC_CLIENT_ID, OIDC_CLIENT_SECRET,
## and OAUTH2_COOKIE_SECRET set in .env.

up-prod: docker-check env-check-auth ## Start production stack (real OIDC — no local Dex)
	$(DC_PROD) up --build -d
	@echo ""
	@echo "  UI (login required): http://localhost"
	@echo "  Traefik dashboard:   http://localhost:8080"
	@echo ""

down-prod: ## Stop production stack
	$(DC_PROD) down

logs-prod: ## Follow logs from production stack
	$(DC_PROD) logs -f

# ── Docker — production with Dex (Google + Apple) ─────────────────────────────
## Docker — production with Dex auth hub (Google + Sign in with Apple)
## Requires DEX_CLIENT_SECRET, OAUTH2_COOKIE_SECRET, GOOGLE_CLIENT_ID,
## GOOGLE_CLIENT_SECRET, APPLE_CLIENT_ID, APPLE_TEAM_ID, APPLE_KEY_ID,
## and APPLE_PRIVATE_KEY set in .env.

up-prod-dex: docker-check env-check-dex-prod ## Start production stack with Dex (Google + Apple login)
	$(DC_PROD_DEX) up --build -d
	@echo ""
	@echo "  UI (login required): https://${HOST:-localhost}"
	@echo "  Dex:                 https://${HOST:-localhost}/dex"
	@echo "  Traefik dashboard:   http://localhost:8080"
	@echo ""

down-prod-dex: ## Stop production Dex stack
	$(DC_PROD_DEX) down

logs-prod-dex: ## Follow logs from production Dex stack
	$(DC_PROD_DEX) logs -f

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

# ClickHouse is an external dependency — configure CLICKHOUSE_HOST in .env

# ── Cleanup ───────────────────────────────────────────────────────────────────
## Cleanup

clean: docker-check ## Remove containers, dangling images, and Python caches
	-$(DC_BASE) down --remove-orphans 2>/dev/null
	-$(DC_AUTH_DEX) down --remove-orphans 2>/dev/null
	docker image prune -f
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	@echo "✓ Clean"
