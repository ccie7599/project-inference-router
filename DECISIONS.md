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
**Status**: accepted (substrate behavior validated 2026-05-28 — see `docs/validation.md`)
**Date**: 2026-05-28
**Context**: Need to land a working end-to-end demo. Substrate HTTP API at `mq.connected-cloud.io` exposes queues, topics, subscriptions, SSE — but no native NATS, no public KV, no per-key TTL.
**Decision**:
- **Router** is a single Spin function (Rust, `wasm32-wasip1`) on FWF behind `inference.connected-cloud.io`. Stateless per request. Reads ephemeral presence state from the Spin default KV store.
- **Work fanout** uses queues — gives the queue-group exactly-once-handler property we need. Queue per model: `inference_req_<normalized_model>`. (Substrate name rule `[A-Za-z0-9_-]{1,64}` forbids dots in names.)
- **Streaming responses** use a shared topic `inference_responses` with subjects `inference.<corr_id>.token|done|error` (dots are fine in subjects). Filtered subscriptions give per-request fan-out without queue-quota blowup.
- **Streaming shape**: router creates SSE subscription, returns `302` to `…/subscriptions/<id>/stream?bearer=…`. Browser holds the connection directly to the substrate, bypassing FWF wall-clock. SSE subscriptions buffer pre-connect messages, so create-sub→publish-work→302→connect is race-free.
- **req/res shape**: router creates an ephemeral per-corr-id queue `inference_resp_<corr_id>`, publishes work, long-polls the queue (max 20s), returns the message body as JSON, deletes the queue. Bounded by FWF 30s wall-clock.
- **Worker dual-publish**: workers always publish tokens + done to the response topic; when `response_queue` is set in the work item, they ALSO publish the assembled response to that queue. Worker code doesn't branch on streaming vs req/res — the router decides by whether it provisioned a queue.
- **Presence**: workers publish heartbeats (every 10s, subject `presence.<id>.<normalized_model>`) to topic `worker_presence`. A long-lived `http_push` subscription forwards each heartbeat (raw body, headers `x-mq-subject|topic|id|attempt|timestamp`) to `POST /v1/internal/presence` on the router, which upserts `{model, worker_id, last_seen}` into Spin KV. Router reads on dispatch; entries older than 30s are treated as gone.
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

## ADR-003: v0.1 deploy shape — own Akamai property, SAN cert reuse, region-less FWF
**Status**: accepted
**Date**: 2026-05-28
**Context**: Need a public-facing demo URL. SCOPE.md left "own property vs piggyback" and "public vs internal" open.
**Decision**:
- **Public-facing** at `https://inference.connected-cloud.io/` (resolves via Edge DNS CNAME → `inference.connected-cloud.io.edgekey.net`).
- **Dedicated Akamai property** `inference.connected-cloud.io` (prp_1362578, contract `ctr_M-1YX7F61`, group `grp_203183` (IOT), product `prd_SPM` (Ion Premier), ruleFormat `latest`). Cloned from sse.connected-cloud.io (prp_1318033) v28 as a starting point. Patches: dropped "Token Auth Gate" (public access), dropped "SSE Streaming" (its nested origin pointed at sse's H2 backend, irrelevant here), dropped the DataStream behavior (stream wasn't accessible to this tenant — to be re-added separately once a CP code mapping is sorted).
- **TLS via SAN cert reuse**: Edge hostname `inference.connected-cloud.io.edgekey.net` (ehn_6162181, ENHANCED_TLS). Cert from CPS enrollment **293468** — Let's Encrypt DV SAN cert with CN `sse.connected-cloud.io` and 24 other SANs. `inference.connected-cloud.io` was already on that SAN list, so no enrollment update / DNS-01 dance was needed.
- **FWF origin**: `<app-id>.fwf.app` URL emitted by `spin aka deploy`. FWF is region-less, so no region pick. Property's default origin behavior uses `forwardHostHeader: ORIGIN_HOSTNAME` so the FWF app routes by Host header correctly.
- **Tooling**: PAPI / CPS / Edge DNS via direct curl with EdgeGrid signing (Python helper). Not Terraform-managed for v0.1 — the gain (one property, one DNS record, reused cert) is small vs. the setup cost. If a second property class shows up, revisit Terraforming via the Akamai provider.
**Rationale**:
- Own property gives a clean independent activation lifecycle (don't have to bundle inference changes with sse releases).
- SAN-cert reuse avoids a multi-hour cert issuance + DNS-01 cycle.
- IOT group + Ion Premier product mirror sse, so behaviors + features available are identical and the clone-then-strip path was straightforward.
**Consequences**:
- Activation happens via direct PAPI calls — the activation record is captured here but the rule tree itself only lives in `prp_1362578` on Akamai. If we ever need to recreate, the procedure is in this ADR's bullets.
- Property is open (no token gate). For demo. v0.2 may need an ACL if we don't want random callers warming up workers.
- Cert SAN list now used by both this property and the others sharing enrollment 293468; lifecycle (renewal, rotation) is shared and not project-scoped.
**Alternatives considered**:
- Issue a new dedicated DV cert for `inference.connected-cloud.io`: rejected — slower, doesn't reuse existing infra.
- Piggyback on existing sse.connected-cloud.io property under a path: rejected — couples release cadence to sse and forces both apps to share the gate / SureRoute config.
- Terraform the whole thing: deferred to when a second property is needed in this project.

## ADR-004: Skip ack + queue delete in the router; reap orphan queues externally
**Status**: accepted (workaround pending upstream investigation)
**Date**: 2026-05-28
**Context**: First end-to-end smoke through Akamai → FWF → substrate consistently 500'd at the 30s wall-clock. Bisected by stripping calls: req/res returned correctly only after BOTH `ack` (DELETE on `/queues/<q>/messages/<token>`) and `delete_queue` (DELETE on `/queues/<q>`) were removed from the handler. Direct curl to the same endpoints returns 204 in ~100ms. Substrate is fine; the worker (using reqwest) does DELETEs all day.
**Decision**:
- The router does **not** ack response-queue messages and does **not** delete response queues. The response-queue path is shaped so neither matters for correctness: the queue is per-corr-id and the next request gets a fresh queue.
- Unacked response-queue messages get DLQ'd after 5 redeliveries × 25 s ack_wait (~125 s); that's acceptable since nothing else is consuming.
- Orphan response queues accumulate at the rate of one per req/res request. They're capped by the 50-queue tenant quota. `scripts/janitor.sh` reaps them from any host that has the bearer; intent is a cron / systemd timer on the worker host, every ~10 min.
- Subscription create (POST) and topic publish (POST) work fine in Spin → FWF. Only outbound DELETE was observed to hang.
**Rationale**:
- The substrate's REST surface is otherwise sane and fast. Punishing this bug to figure out *why* Spin's DELETE hangs (a wasi-http vs server-side TLS-keepalive interaction? an empty-body content-length quirk? a Spin SDK 3.1 bug?) is a yak-shave we don't need to take for a v0.1 demo.
- The workaround is contained: only ack + delete are skipped; everything else (presence, publish, receive, subscription create, 302 redirect) works as designed.
**Consequences**:
- Need an external janitor or things will pile up. Without it: at ~10 req/min sustained, the 50-queue quota is hit in ~5 minutes.
- DLQ traffic for unacked response-queue messages adds substrate work. Doesn't affect correctness.
- Streaming-shape requests are unaffected (no per-corr-id queue is created).
**Open**:
- Reproduce in a minimal Spin component and report upstream — `spin_sdk::http::send` with `Method::Delete` to a vanilla HTTPS endpoint, does it hang? If yes, file with Fermyon. If no, narrow further (TLS keepalive? response body framing?).
- Try the lower-level `spin-sdk` outbound primitives instead of `http::send`.
- Try issuing the DELETE as `POST` to a different route on the substrate if one exists (it doesn't today; would need substrate-side support).
**Alternatives considered**:
- Have the worker delete the response queue after publishing the final response. Cleaner architecturally but mixes concerns and still leaves ack on the router side. Worth doing once the upstream bug is investigated.
- Pre-create a pool of response queues and round-robin them. Avoids per-request create but needs router-side state across invocations; not a fit for a stateless function.
