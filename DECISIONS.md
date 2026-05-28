# DECISIONS — project-inference-router

Architecture Decision Records. Add one entry per non-trivial choice.

## Format

```
## ADR-NNN: <one-line title>
**Status**: proposed | accepted | rejected | superseded
**Date**: YYYY-MM-DD
**Context**: what problem we're solving
**Decision**: what we chose
**Rationale**: why this beat the alternatives
**Consequences**: what this commits us to (good and bad)
**Alternatives considered**: with one-line reasons we passed
```

## ADR-001 (placeholder): Adopt NATS `micro` framework conventions for worker registration
**Status**: deferred to v0.2
**Date**: 2026-05-28
**Context**: We need a worker-registration + request-routing + queue-group load-balancing pattern. NATS already ships a productized version: `github.com/nats-io/nats.go/micro`.
**Decision**: Deferred. Native NATS access is not exposed publicly today; v0.1 uses the HTTP API only (see ADR-002). Revisit when the NKey path opens.
**Rationale**: Reinventing this would be slower and produce a less idiomatic API for anyone already on NATS, but we can't adopt `micro` without native NATS access.
**Consequences**: v0.1 invents its own thin presence + queue-group analog over HTTP. Documented as transitional.
**Alternatives considered**: Custom queue-group + custom presence bucket (chose this for v0.1 — what we ended up doing).

## ADR-002: v0.1 architecture — Spin router + HTTP-only substrate + topic-based responses
**Status**: accepted
**Date**: 2026-05-28
**Context**: Need to land a working end-to-end demo. Substrate HTTP API at `mq.connected-cloud.io` exposes queues, topics, subscriptions, SSE — but no native NATS, no public KV, no per-key TTL.
**Decision**:
- **Router** is a single Spin function (Rust, `wasm32-wasip1`) on FWF behind `inference.connected-cloud.io`. Stateless per request. Reads ephemeral presence state from the Spin default KV store.
- **Work fanout** uses queues (`inference.{model}.req`) — gives the queue-group exactly-once-handler property we need.
- **Streaming responses** use a shared topic `inference.responses` with subjects `inference.<corr_id>.token|done|error`. Filtered subscriptions give per-request fan-out without queue-quota blowup.
- **Streaming shape**: router creates SSE subscription, returns `302` to `https://mq.connected-cloud.io/v1/subscriptions/<id>/stream?bearer=…`. Browser holds the connection directly to the substrate, bypassing FWF wall-clock.
- **req/res shape**: router creates an ephemeral per-corr-id queue `inference.resp.<corr_id>`, publishes work, long-polls the queue, returns JSON, deletes the queue. Bounded by FWF 30s wall-clock.
- **Worker dual-publish**: workers always publish tokens + done to the response topic; when `response_queue` is set in the work item, they ALSO publish the assembled response to that queue. Worker code doesn't branch on streaming vs req/res — the router decides by whether it provisioned a queue.
- **Presence**: workers publish heartbeats (every 10s, subject `worker.<id>.<model>`) to a topic `worker.presence`. A long-lived `http_push` subscription forwards each heartbeat to `POST /v1/internal/presence` on the router, which upserts `{model, worker_id, last_seen}` into Spin KV. Router reads on dispatch; entries with `last_seen > 30s` ago are treated as gone.
**Rationale**:
- Topic+filter is the only response shape that scales without burning queues per request (50-queue quota).
- Spin KV is the only stateful primitive available to a FWF function for the presence snapshot (no native KV REST in substrate).
- http_push subscription → router endpoint is a clean inversion of the "router subscribes" model, which a stateless function can't do.
**Consequences**:
- Spin KV becomes a critical runtime dependency. If unavailable on the FWF deployment target, fall back to an external lightweight presence store.
- Streaming shape: one `inference.responses` topic + N subscriptions per in-flight request. Subscription cleanup happens at disconnect (auto-expire per substrate docs).
- req/res shape: one ephemeral queue per in-flight request. Capped by 50-queue quota → max 50 concurrent req/res calls. Router deletes queue after consuming. Janitor for orphaned queues deferred to v0.2.
- Worker authors don't have to know req/res vs streaming — they always publish tokens to the topic, and conditionally publish the assembled response to the queue if the work item specifies one.
- Router req/res handler is bounded by FWF 30s — long inferences MUST use streaming.
**Alternatives considered**:
- Router consumes the topic SSE inside the function for req/res: rejected — Spin's `send` doesn't compose well with long-lived chunked responses; per-corr-id queue is simpler.
- Single shared response queue with corr_id in body: rejected — consume-once semantics race across in-flight requests.
- Workers HTTP-POST presence to router: rejected — user picked topic-based; topic also gives observability for free.
- TypeScript Spin function: rejected — Brian prefers Rust for performance-sensitive code; router IS the front-door.
