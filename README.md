# project-inference-router

Edge-native distributed inference router. Akamai CDN + FWF function as the request router + Akamai Cloud Queues + Notifications (`mq.connected-cloud.io`) for the work fabric + heterogeneous customer-owned inference workers connected via NATS.

**Status**: v0.1 scaffold landed (router + worker + bootstrap) — needs end-to-end smoke run against the live substrate.

**Started from**: a "what could we build as a reference app on Akamai Cloud Queues + Notifications" conversation on 2026-05-28. The headline concept was already sketched in `project-nats-mq/SCOPE.md` §426 — that text is preserved here in `docs/concept.md`.

**Sibling/dependency**: `~/project-nats-mq` — the Queues + Notifications service this router uses as its work fabric. Both run against the same 27-region NATS JetStream substrate. The tenant + bearer this project uses is in `.tenant-bearer` (gitignored) — paste it as the `Authorization: Bearer …` header on every API call.

## Why this is interesting

- **AWS can't match it cleanly.** Bedrock owns the workers; SageMaker MME is single-region; nobody else lets a customer keep their inference runtime under their own control while still getting global front-door + routing fabric.
- **Zero new substrate features needed.** The pattern is built entirely on what nats-mq already exposes (queues, topics, SSE, http_push, NATS-direct for workers). It's a *demo on top of v1*, not a platform extension.
- **Connects existing Akamai portfolio demos** — `mortgage-inference` (Phi-3-mini at edge) and `step-inside-reco` become registered worker classes in this router.
- **Composable with OHTTP-2** — fronting this with OHTTP gives "confidential distributed inference" for fintech/healthcare.

## Where to start

1. `SCOPE.md` — v0.1 exit criteria (locked).
2. `DECISIONS.md` ADR-002 — the architecture choices and why.
3. `docs/architecture.md` — concrete wire protocol (endpoints, queues, topics, message shapes).
4. `docs/concept.md` — original architectural sketch (kept for context).
5. `docs/using-nats-mq.md` — how the substrate works.

## Running v0.1

Prereqs:
- Rust + `cargo` with the `wasm32-wasip1` target (`rustup target add wasm32-wasip1`)
- `spin` CLI (`fermyon/spin` >= 3.0)
- A local Ollama with the model pulled: `ollama pull llama3.2:1b`
- `.tenant-bearer` at the repo root (already in place)
- A way for the substrate to POST presence heartbeats back to your router URL. For local dev: `ngrok http 3000` and pass the public URL to `ROUTER_PRESENCE_URL`.

Three terminals:

```bash
# Terminal 1 — bootstrap substrate (once per fresh tenant)
make bootstrap-substrate ROUTER_PRESENCE_URL=https://<your-tunnel>.ngrok.app/v1/internal/presence

# Terminal 2 — run the worker
make run-worker

# Terminal 3 — run the router (Spin on localhost:3000)
make run-router
```

Smoke test:

```bash
make smoke
```

You should see the req/res JSON response in stdout, and then a streaming SSE feed from the substrate showing token-by-token output ending in an `inference.<corr_id>.done` event.

## Layout

```
router/   Spin function (Rust, wasm32-wasip1) — the request router
worker/   Native Rust binary — Ollama-backed inference worker
docs/     concept.md, architecture.md, using-nats-mq.md
scripts/  bootstrap-substrate.sh, smoke.sh
```

## Pointers

- Service base URL: `https://mq.connected-cloud.io`
- Service docs: `https://mq.connected-cloud.io/docs` (architecture), `/guide` (user guide), `/api-explorer` (Swagger UI)
- Tenant ID: see `docs/using-nats-mq.md`
- Sibling repo: `~/project-nats-mq`
- Prior art to skim before designing: the NATS `micro` framework (`github.com/nats-io/nats.go/micro`) — service registration + request routing + queue-group LB + statistics, productized by the NATS team for exactly this shape. Adopt or wrap their conventions rather than reinvent.
