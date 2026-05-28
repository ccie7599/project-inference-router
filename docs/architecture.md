# Architecture — v0.1

Concrete wire protocol for the v0.1 cut. See `DECISIONS.md` (ADR-002) for the *why*; this doc is the *what*.

## Components

```
┌────────────────────┐    POST /v1/inference     ┌────────────────────┐
│  Client (browser   │ ─────────────────────────▶│  Router            │
│  or curl)          │                            │  Spin function on  │
│                    │ ◀───── 302 (stream)        │  FWF (Rust)        │
│                    │       or  200 JSON         │                    │
└────────────────────┘                            └─────────┬──────────┘
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

## Substrate resources

Created once at deploy time (see `scripts/bootstrap-substrate.sh`):

| Kind         | Name                            | Purpose                                                      |
|--------------|---------------------------------|--------------------------------------------------------------|
| Queue        | `inference.llama3-2-1b.req`     | Work queue for the `llama3.2:1b` model. Queue-group LB.      |
| Topic        | `inference.responses`           | All response tokens. Subjects scope to corr-id.              |
| Topic        | `worker.presence`               | Worker heartbeats. Subjects scope to worker-id + model.      |
| Subscription | `presence-collector`            | `worker.presence` → `http_push` to router presence endpoint. |

Per-request (created lazily by router on each call):

| Kind         | Name pattern                          | Used by      | Lifetime                                  |
|--------------|---------------------------------------|--------------|-------------------------------------------|
| Subscription | filter `inference.<corr_id>.*` on `inference.responses` | streaming    | Auto-expires on client disconnect.        |
| Queue        | `inference.resp.<corr_id>`            | req/res      | Router creates pre-publish, deletes post-receive. |

**Queue naming**: model name normalized to substrate-safe form (`llama3.2:1b` → `llama3-2-1b`). Reserved characters: `:` `.` not in queue names; both replaced with `-`.

## Router endpoints

### `POST /v1/inference`

Request body:
```json
{
  "model": "llama3.2:1b",
  "prompt": "Why is the sky blue?",
  "stream": false
}
```

`stream` defaults to `false`. Behavior split:

#### `stream: false` (req/res)
1. Generate `corr_id = "corr_<uuid-v4>"`.
2. Look up workers for `model` in Spin KV. If none fresh (heartbeat ≤ 30s old), return `503 no-workers`.
3. Create response queue `inference.resp.<corr_id>` via `POST /queues`.
4. Publish request to queue `inference.<normalized-model>.req` with body:
   ```json
   {
     "corr_id": "corr_…",
     "model": "llama3.2:1b",
     "prompt": "…",
     "response_queue": "inference.resp.corr_…"
   }
   ```
5. Long-poll `GET /queues/inference.resp.<corr_id>/receive?wait=25` (FWF budget − margin).
6. Ack the message, then `DELETE /queues/inference.resp.<corr_id>`.
7. Return the message body to the caller (worker already shaped it as the final response JSON).
8. On timeout / error: return `504`. Best-effort delete the queue.

#### `stream: true`
1. Generate `corr_id`.
2. Look up workers (same as above).
3. Create subscription on `inference.responses` filtered to `inference.<corr_id>.*`, delivery `sse`.
4. Return `302 Found` with `Location: https://mq.connected-cloud.io/v1/subscriptions/<sub-id>/stream?bearer=<bearer>`. **Do not publish the work item yet.**
5. The client follows the 302 and opens the SSE connection. Once connected, the substrate is buffering for that filter.
6. Wait... we can't wait. The function returns at step 4.

Race: a fast worker can publish tokens before the browser is connected and the substrate begins delivery, causing the browser to miss the start. Mitigation in v0.1: the router does a brief sleep (200ms) AND publishes the work item, before returning the 302. The 302 round-trip + browser connect takes longer than that, but in the substrate the subscription is created at step 3 and starts buffering — `delivery.type: sse` subscriptions buffer pending messages and replay on connect. (Verify against substrate semantics; fall back to publishing-after-connect via a tiny pre-fetch endpoint if buffering proves not to work.)

Concretely for v0.1: order is **create-sub → publish-work → 302**. The buffering window between publish and browser-connect is what saves us.

### `POST /v1/internal/presence`

http_push target from the `presence-collector` subscription. Substrate POSTs each heartbeat here.

Body (one heartbeat, as published by worker):
```json
{
  "worker_id": "wkr_<uuid>",
  "model": "llama3.2:1b",
  "region": "local-dev",
  "ts": 1748449698
}
```

Router upserts a key `presence:<model>:<worker_id>` → `{model, worker_id, region, last_seen}` in Spin KV. No TTL — staleness is checked on read.

This endpoint trusts the substrate. In v0.2 we add a shared secret in the http_push header.

### `GET /healthz`
Returns `200 ok\n`. Used by FWF health checks.

## Worker behavior

```
on startup:
  worker_id = "wkr_" + uuid4()
  spawn heartbeat task (every 10s):
    POST topic worker.presence,
      X-MQ-Subject: worker.<id>.<model>
      body: {"worker_id":..., "model":..., "region":..., "ts": now}
  loop:
    msg = receive(queue=inference.<normalized-model>.req, wait=30s)
    if not msg: continue
    {corr_id, prompt, response_queue} = parse(msg.body)
    accumulator = ""
    token_count = 0
    stream = ollama.generate(model, prompt, stream=true)
    for token in stream:
      accumulator += token
      token_count += 1
      publish(topic=inference.responses,
              subject=inference.<corr_id>.token,
              body={"token": token})
    publish(topic=inference.responses,
            subject=inference.<corr_id>.done,
            body={"tokens": token_count})
    if response_queue:
      send(queue=response_queue,
           body={"corr_id": corr_id, "model": model,
                 "response": accumulator, "tokens": token_count})
    ack(msg)
```

Errors during inference → publish to subject `inference.<corr_id>.error`, send error JSON to `response_queue` if present, ack the message (we don't redrive — a fresh request is the user's problem).

## Authentication

- Router → substrate: `Authorization: Bearer <tenant-bearer>` (read at startup from Spin variable `tenant_bearer`, sourced from `.tenant-bearer`).
- Worker → substrate: same bearer.
- Browser → substrate (streaming case): `?bearer=…` query string. Same bearer. (For v0.1 this leaks the bearer to the browser — acceptable for a single-tenant POC. v0.2: short-lived signed token.)
- Client → router: open. Edge ACL (Akamai property) can restrict source IPs in deploy step.

## Failure modes & their handling

| Condition                              | Behavior                                                          |
|----------------------------------------|-------------------------------------------------------------------|
| No fresh workers for model             | `503 no-workers` immediately                                      |
| Substrate API error on sub create      | `502 substrate-error` with detail                                 |
| Worker dies mid-stream                 | Streaming caller's SSE just stops. Req/res caller times out at 25s, returns `504`. |
| Token never arrives within 25s         | `504` with whatever fragments were collected                      |
| http_push from substrate replays heartbeat | Idempotent — upsert by `(model, worker_id)`                   |
| Spin KV unavailable                    | `500 presence-store-unavailable`. Router has no fallback in v0.1. |

## Things this v0.1 doesn't do (revisit in v0.2)

- Per-region worker selection (just picks first fresh worker for the model)
- Capacity-aware dispatch (no `current_load` field consulted)
- Retry on worker failure
- Worker auth beyond shared bearer
- Cost / token accounting
- Multiple worker classes (only `llama3.2:1b`)
- Native NATS path with `micro` framework
