# SCOPE — project-inference-router

**Tier**: 1 (research POC / reference application).
**Status**: v0.1 scope locked 2026-05-28.
**Created**: 2026-05-28.

This is a reference application demonstrating what's buildable on the Akamai Cloud Queues + Notifications platform. Audience for the finished demo: internal Fermyon + Akamai cloud architects/SMEs evaluating the platform.

## Purpose

Show that a customer can keep their inference runtime under their own control while still getting an Akamai global front-door + routing fabric, with both request/response and streaming-tokens response shapes, across heterogeneous worker types (Triton, vLLM, Ollama, Phi-3 at edge, anything that speaks HTTP).

## v0.1 exit criteria (locked 2026-05-28)

- [x] Architecture written up in `DECISIONS.md` (ADR-002) and `docs/architecture.md`
- [x] Substrate wire shapes validated against the live service (`docs/validation.md`)
- [ ] One inference worker class running end-to-end: local Ollama (`llama3.2:1b`)
- [ ] Request router as a Spin function on FWF (Rust, `wasm32-wasip1`)
- [x] Hostname routed via Akamai (`inference.connected-cloud.io`) — Akamai property prp_1362578, edge hostname `inference.connected-cloud.io.edgekey.net` (ehn_6162181, ENHANCED_TLS via CPS enrollment 293468 SAN cert), CNAME live in Edge DNS, activation pending at time of doc
- [ ] req/res response shape working end-to-end (router collects SSE, returns JSON)
- [ ] streaming-tokens response shape working end-to-end (router 302s to substrate SSE)
- [ ] Worker presence in Spin KV, refreshed via http_push subscription on `worker.presence` topic
- [ ] Smoke test script that exercises both shapes

Out of v0.1 (queued for v0.2):
- Second worker class
- Native NATS path for workers (NKey + `micro` framework)
- Per-region routing

## Non-goals (explicit)

- Not building a managed inference platform. The point is to demonstrate the *pattern* — customers run their own workers.
- Not building a model registry or version-management system.
- Not building authentication beyond what nats-mq already provides (tenant bearer for API callers, NKey for workers).
- Not building rate limiting / quota enforcement beyond what the substrate already enforces.
- Not building billing / metering.
- Not building a model marketplace.

## Dependencies

- `~/project-nats-mq` — the messaging substrate. Must be running and reachable at `https://mq.connected-cloud.io`.
- Inference worker(s) — at least one running somewhere (LKE, bare metal, laptop, doesn't matter as long as it can reach NATS).
- Optionally: `~/project-latency`, `~/project-landing-zone` for placement and DS2 integration if this expands to a 27-region demo.

## Open questions

Resolved for v0.1:
- ~~Single hostname or per-model?~~ → **Single** `inference.connected-cloud.io`, model in request body.
- ~~Worker auth?~~ → **Tenant bearer for HTTP** (the substrate's existing path); workers don't authenticate to router. Native NATS / NKey deferred to v0.2.
- ~~Response shape?~~ → **Both** req/res and streaming.
- ~~Presence?~~ → **Topic heartbeat → http_push subscription → Spin KV** (see ADR-002).

Still open:
- ~~Own Akamai property + cert vs piggyback?~~ → **Own property** `inference.connected-cloud.io` (prp_1362578), SAN cert reused from CPS enrollment 293468 (covers sse + 23 other hostnames). See ADR-003.
- ~~Where does the demo land?~~ → **Public-facing** at `https://inference.connected-cloud.io/`.

## Tier-2 graduation triggers (when this stops being a POC)

- Multiple customer demo conversations want it as a starting point — promote to Tier 2 with HANDOFF.md + runbook.
- Fermyon picks it up — co-maintenance shape needs documenting.
- Becomes part of a sales-stage demo flow — needs customer-brief.md.
