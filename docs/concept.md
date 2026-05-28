# Concept — Distributed Inference Router at the Edge

Captured verbatim (light edits) from `~/project-nats-mq/SCOPE.md` §426 on 2026-05-28. This is the canonical architectural sketch that motivated the project.

---

**Use case**: customer points `inference.<their-domain>` at Akamai. CDN routes to an FWF function (the request router). Function dispatches the request through MQ (`mq.connected-cloud.io`) to a fleet of inference workers (customer-owned, anywhere — k8s, bare-metal, Triton, vLLM, Ollama, custom) that have registered themselves on the mesh. Response either returns directly (req/res, e.g. ranking, embeddings, small models) or streams tokens back to the user via SSE (LLMs, image gen progress).

## Both response shapes

### req/res

- Function POSTs request to `inference.<model>.req` queue (queue-group → exactly one worker handles)
- Polls `inference.<corr-id>.resp` queue with long-poll up to 20s (fits FWF 30s budget)
- Returns to user

### streaming

- Function calls `mq::sse_redirect("inference.<corr-id>.tokens")`
- Returns 302 to the adapter SSE endpoint (`https://mq.connected-cloud.io/v1/subscriptions/<id>/stream?bearer=<token>`)
- Browser holds long-lived connection to adapter (bypasses FWF wall-clock entirely, per the Option C routing already in place)
- Function (separately) publishes the request to `inference.<model>.req`
- Worker streams tokens to the corr-id topic
- Adapter fans them to the user's SSE in real time

Uses the SSE pattern already shipped in nats-mq v1.

## Worker registration

- Workers connect to NATS via NKey+JWT (native protocol path — direct, not HTTP)
- Subscribe to `inference.<model>.req` with a queue group (free load balancing across worker pool)
- Publish presence + capabilities to `nats-kv` bucket `workers.<id>` → `{model, region, gpu_type, max_context, current_load, ...}`
- Health TTL on the presence key (per-key TTL when added to nats-kv; otherwise heartbeat)
- Function router reads bucket to know what's available before dispatching

## Geographic placement

Workers in jp-tyo serve jp-tyo requests if they have the model. Same `geo:auto` story as KV/MQ — substrate routes naturally. Router checks the presence bucket scoped to the calling region, picks the closest worker with the requested model + sufficient capacity.

## Prior art

This pattern IS the NATS `micro` framework (`github.com/nats-io/nats.go/micro`) — service registration + request routing + queue-group load balancing + statistics, productized by the NATS team for exactly this shape. We'd adopt or wrap their conventions rather than reinvent.

Adjacent / for-comparison:
- **NVIDIA Triton** — single-server multi-model, no fleet abstraction
- **Ray Serve** — Python-heavy, opinionated runtime
- **KServe** — k8s-coupled, no multi-cluster fleet
- **LiteLLM** — single-node proxy
- **AWS SageMaker MME** — AWS-only, no streaming, no customer-owned workers

The distinguishing thing about this: **Akamai edge front-door + customer-owned inference workers anywhere, connected via NATS**. Nobody else lets a customer keep their inference runtime under their control while still getting global front-door + routing fabric.

## Connects existing portfolio demos

- `mortgage-inference` (Phi-3-mini at edge) becomes a registered worker class
- `step-inside-reco` becomes another worker class
- Could front the entire stack with `OHTTP-2` for privacy-preserving routing — confidential distributed inference, multi-tenant. That's a fintech/healthcare story that lands hard.

## Why this is a reference app, not a platform feature

Zero new substrate features needed (queues, topics, SSE redirect, HTTP push, KV presence — all in nats-mq v1). It's a *demo on top of the v1 platform*, not a platform feature. SCOPE original sizing: ~1 week to build once v1 stabilizes (which is now).
