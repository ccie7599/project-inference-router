# Architecture — v0.1

Concrete wire protocol for the v0.1 cut. Substrate behavior validated live on 2026-05-28 — see `docs/validation.md`. See `DECISIONS.md` (ADR-002) for the *why*; this doc is the *what*.

## Components

```
┌────────────────────┐
│  Client (browser   │
│  or curl)          │
└─────────┬──────────┘
          │ POST https://inference.connected-cloud.io/v1/inference
          ▼
┌────────────────────────────────────────────┐
│  Akamai property inference.connected-cloud │
│  Ion Premier (prp_1362578); SAN cert from  │
│  CPS enrollment 293468                     │
└─────────┬──────────────────────────────────┘
          │ HTTPS to origin <app-id>.fwf.app
          ▼
┌────────────────────┐
│  Router            │
│  Spin function on  │
│  FWF (Rust)        │
│                    │
│                    │ ◀───── 302 (stream)
│                    │       or  200 JSON
└─────────┬──────────┘
        ▲                                                   │
        │ SSE (streaming case only,                         │ HTTP
        │ direct to substrate, bypasses FWF)                │
        │                                                   ▼
        │                                  ┌──────────────────────────────┐
        │                                  │  mq.connected-cloud.io       │
        └──────────────────────────────────│  Cloud Queues + Notif.       │
                                           │  (tenant t_daed…)            │
                                           └──┬───────────────────────┬───┘
                                              │                       │
                                  HTTP poll   │                       │ HTTP push
                                  receive     ▼                       ▼
                                    ┌──────────────────┐    ┌──────────────────┐
                                    │  Worker          │    │  Router          │
                                    │  (Rust + Ollama) │    │  /v1/internal/   │
                                    │  on laptop / LKE │    │  presence        │
                                    └──────────────────┘    └──────────────────┘
                                              │
                                              ▼
                                    ┌──────────────────┐
                                    │  Ollama          │
                                    │  llama3.2:1b     │
                                    │  localhost:11434 │
                                    └──────────────────┘
```

## Substrate naming rule

Queue and topic **names** must match `[A-Za-z0-9_-]{1,64}` — no dots, no colons. **Subjects** (used for routing and filters) DO accept dots and follow standard NATS hierarchy. This is the reason names use `_` separators below while subjects use `.`.

## Substrate resources

Created once at deploy time (see `scripts/bootstrap-substrate.sh`):

| Kind         | Name                          | Purpose                                                      |
|--------------|-------------------------------|--------------------------------------------------------------|
| Queue        | `inference_req_llama3-2-1b`   | Work queue for the `llama3.2:1b` model. Queue-group LB.      |
| Topic        | `inference_responses`         | All response tokens. Subjects scope to corr-id.              |
| Topic        | `worker_presence`             | Worker heartbeats. Subjects scope to worker-id + model.      |
| Subscription | (no specific name)            | `worker_presence` filter `presence.>` → `http_push` to router presence endpoint. |

Per-request (created lazily by the router):

| Kind         | Name pattern                          | Used by   | Lifetime                                          |
|--------------|---------------------------------------|-----------|---------------------------------------------------|
| Subscription | filter `inference.<corr_id>.>` on `inference_responses` | streaming | Auto-expires on client disconnect.                |
| Queue        | `inference_resp_<corr_id>`            | req/res   | Router creates pre-publish, deletes post-receive. |

**Model-name normalization**: `llama3.2:1b` → `llama3-2-1b` (every non-`[A-Za-z0-9_-]` becomes `-`). `corr_id` is `corr_<uuid32hex>` so it's already substrate-name-safe.

## Substrate wire shapes (validated 2026-05-28)

- **Send to queue**: `POST /queues/{name}/messages`, `Content-Type: application/octet-stream`, raw bytes body. We send JSON-encoded as bytes.
- **Receive from queue**: `GET /queues/{name}/messages?wait=N` (max **N=20**). Returns 200 with `{count, messages:[{ack_token, body_b64, msg_id, ...}]}`. Body is base64. Empty queue: `count:0, messages:[]` (NOT 204).
- **Ack**: `DELETE /queues/{name}/messages/{ack_token}` → 204. Unacked messages redeliver after 25s (`ack_wait`); after 5 deliveries they go to DLQ.
- **Topic publish**: `POST /topics/{name}/publish`, `Content-Type: application/json`, body is the message JSON, with `X-MQ-Subject: <subject>` header carrying the routing subject (dots OK).
- **Subscription create**: `POST /subscriptions` `{topic, filter?, delivery:{type, url?}}` → 201 with `{id, consumer, ...}`. `filter` is leaf-form (`inference.<corr_id>.>`); substrate prefixes it internally.
- **SSE stream**: `GET /subscriptions/{id}/stream?bearer=<token>`. Pre-connect messages **are** buffered for delivery.
- **http_push delivery**: substrate POSTs raw published bytes to your URL with `application/octet-stream` and headers `x-mq-subject`, `x-mq-topic`, `x-mq-id`, `x-mq-attempt`, `x-mq-timestamp`. No envelope.

## Router endpoints

### `POST /v1/inference`

Request:
```json
{
  "model": "llama3.2:1b",
  "prompt": "Why is the sky blue?",
  "stream": false
}
```

`stream` defaults to `false`.

#### `stream: false` (req/res)
1. `corr_id = "corr_<uuid-v4-simple>"`.
2. Look up workers for `model` in Spin KV. If none fresh (heartbeat ≤ 30s old), return `503 no-workers`.
3. `POST /queues` to create `inference_resp_<corr_id>` (201 even if it already exists).
4. `POST /queues/inference_req_<normalized-model>/messages` with body:
   ```json
   {"corr_id":"corr_…","model":"llama3.2:1b","prompt":"…","response_queue":"inference_resp_corr_…"}
   ```
5. `GET /queues/inference_resp_<corr_id>/messages?wait=20`. Decode `body_b64`. Ack via `DELETE`.
6. `DELETE /queues/inference_resp_<corr_id>` (best-effort).
7. Return the decoded body as JSON to the caller — the worker shaped it as the final response.
8. On timeout (empty receive): `504`. On substrate error: `502`. Queue is deleted in both cases.

#### `stream: true`
1. `corr_id`.
2. Worker presence check (same).
3. `POST /subscriptions` filter `inference.<corr_id>.>` on `inference_responses`, delivery `sse` → grab `id`.
4. Publish the work item (no `response_queue` field).
5. Return `302 Found` with `Location: https://mq.connected-cloud.io/v1/subscriptions/<id>/stream?bearer=<bearer>`.

Order is intentional: subscription before publish. Since substrate buffers pre-connect, the worker can also publish before the browser connects — no race.

### `POST /v1/internal/presence`

http_push target from the presence-collector subscription. The body is the JSON the worker published; deserialize directly:
```json
{"worker_id":"wkr_…","model":"llama3.2:1b","region":"local-dev","ts":1748449698}
```

Router upserts `presence:<normalized-model>:<worker_id>` → `PresenceRecord` in Spin KV. No TTL; staleness checked on read.

The endpoint trusts the substrate. v0.2 adds a shared secret in an `Authorization` or custom header that the subscription sends.

### `GET /healthz`
`200 ok\n`.

## Worker behavior

```
on startup:
  worker_id = "wkr_" + uuid4_simple()
  ensure resources (idempotent create of req queue + response topic + presence topic)
  spawn heartbeat task (every 10s):
    POST topics/worker_presence/publish,
      X-MQ-Subject: presence.<worker_id>.<normalized_model>,
      body: {"worker_id":..., "model":..., "region":..., "ts": now}
  loop:
    messages = GET queues/inference_req_<normalized_model>/messages?wait=20
    for msg in messages:
      decode body_b64 -> WorkItem {corr_id, prompt, response_queue}
      accumulator = ""; token_count = 0
      stream = ollama.generate(model, prompt, stream=true)
      for token in stream:
        accumulator += token; token_count += 1
        POST topics/inference_responses/publish
          X-MQ-Subject: inference.<corr_id>.token
          body: {"token": token}
      POST topics/inference_responses/publish
        X-MQ-Subject: inference.<corr_id>.done
        body: {"tokens": token_count}
      if response_queue:
        POST queues/<response_queue>/messages (octet-stream)
          body: {"corr_id":..., "model":..., "response": accumulator, "tokens": token_count}
      DELETE queues/inference_req_<normalized_model>/messages/<ack_token>
```

Errors during inference → publish to subject `inference.<corr_id>.error` and to the response_queue if set. Ack the message either way (no redrive in v0.1).

## Authentication

- Router → substrate: `Authorization: Bearer <tenant-bearer>` (from Spin variable).
- Worker → substrate: same bearer.
- Browser → substrate (streaming case): `?bearer=…` in the SSE URL. **This leaks the bearer to the browser.** Acceptable for a single-tenant POC; v0.2 issues a short-lived signed token via `?session=`.
- Client → router: open. Edge ACL goes on the Akamai property in the deploy step.

## Failure modes

| Condition | Behavior |
|---|---|
| No fresh workers for model | `503 no-workers` immediately |
| Substrate API error on create/publish | `502` with detail |
| Worker dies mid-stream | Streaming caller's SSE stops. Req/res caller times out at 20s → `504` (router deletes the response queue). |
| Worker never finishes within 20s | `504`; substrate will keep buffering on the topic but the response-queue path doesn't reflect that. |
| http_push retries a heartbeat (`x-mq-attempt` ≥ 2) | Idempotent — router upsert by `(model, worker_id)` |
| Spin KV unavailable | `500 presence-store-unavailable`. No fallback in v0.1. |

## Deferred to v0.2

- Per-region worker selection (today: most-recently-seen wins).
- Capacity-aware dispatch.
- Retry on worker failure.
- Worker auth to the presence endpoint.
- Short-lived signed SSE token (`?session=`).
- Orphan response-queue janitor for crash-mid-request.
- Multiple worker classes (today: only `llama3.2:1b`).
- Native NATS path with `micro` framework.
