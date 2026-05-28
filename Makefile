# project-inference-router — v0.1
#
# Common targets. Override via env or `make VAR=value target`.

SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

MODEL              ?= llama3.2:1b
OLLAMA_BASE        ?= http://localhost:11434
SUBSTRATE_BASE     ?= https://mq.connected-cloud.io/v1
ROUTER_URL         ?= http://localhost:3000
# Default presence URL assumes the substrate can reach your local router.
# For a real run, expose the router (e.g. ngrok / Akamai property / public LKE ingress)
# and override this on the bootstrap-substrate target.
ROUTER_PRESENCE_URL ?= $(ROUTER_URL)/v1/internal/presence

.PHONY: help
help:
	@awk 'BEGIN {FS = ":.*##"; printf "Targets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  %-22s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# ----------------------------------------------------------------------------
# Build
# ----------------------------------------------------------------------------

.PHONY: build
build: build-router build-worker  ## Build router (wasm) + worker (native binary)

.PHONY: build-router
build-router:  ## cargo build the router as wasm32-wasip1
	cd router && cargo build --target wasm32-wasip1 --release

.PHONY: build-worker
build-worker:  ## cargo build the worker (native)
	cd worker && cargo build --release

# ----------------------------------------------------------------------------
# Run
# ----------------------------------------------------------------------------

.PHONY: run-router
run-router: build-router  ## Run router locally with `spin up`
	cd router && spin up \
	  --variable tenant_bearer="$$(cat ../.tenant-bearer | tr -d '[:space:]')" \
	  --variable substrate_base="$(SUBSTRATE_BASE)"

.PHONY: run-worker
run-worker: build-worker  ## Run a worker pointing at local Ollama
	cd worker && cargo run --release -- \
	  --model "$(MODEL)" \
	  --ollama-base "$(OLLAMA_BASE)" \
	  --substrate-base "$(SUBSTRATE_BASE)"

# ----------------------------------------------------------------------------
# Substrate
# ----------------------------------------------------------------------------

.PHONY: bootstrap-substrate
bootstrap-substrate:  ## Create queues, topics, presence-collector sub on the substrate
	MODEL="$(MODEL)" \
	SUBSTRATE_BASE="$(SUBSTRATE_BASE)" \
	ROUTER_PRESENCE_URL="$(ROUTER_PRESENCE_URL)" \
	  bash scripts/bootstrap-substrate.sh

.PHONY: list-substrate
list-substrate:  ## List queues + subscriptions on the tenant
	@BEARER="$$(cat .tenant-bearer | tr -d '[:space:]')"; \
	  echo "== queues =="; \
	  curl -sS -H "Authorization: Bearer $$BEARER" $(SUBSTRATE_BASE)/queues; echo; \
	  echo "== subscriptions =="; \
	  curl -sS -H "Authorization: Bearer $$BEARER" $(SUBSTRATE_BASE)/subscriptions; echo

# ----------------------------------------------------------------------------
# Test / smoke
# ----------------------------------------------------------------------------

.PHONY: smoke
smoke:  ## End-to-end smoke test (req/res + streaming) against $(ROUTER_URL)
	ROUTER_URL="$(ROUTER_URL)" MODEL="$(MODEL)" bash scripts/smoke.sh

.PHONY: check
check:  ## cargo check both crates
	cd router && cargo check --target wasm32-wasip1
	cd worker && cargo check

# ----------------------------------------------------------------------------
# Cleanup
# ----------------------------------------------------------------------------

.PHONY: clean
clean:  ## cargo clean both crates
	cd router && cargo clean
	cd worker && cargo clean
