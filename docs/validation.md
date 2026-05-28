# Substrate validation — 2026-05-28

We ran a series of live probes against `https://mq.connected-cloud.io/v1` with the project's tenant bearer to confirm the assumptions baked into the v0.1 code. This doc captures the *observed* substrate behavior. When future code makes assumptions about the substrate, check here first; if not covered, probe and add a section.

Findings drove a substantial rewrite of v0.1 — see the commit that landed alongside this doc.

## Quick verification

```bash
BEARER=$(tr -d '[:space:]' < .tenant-bearer)
curl -sS -H "Authorization: Bearer $BEARER" https://mq.connected-cloud.io/v1/queues
# → {"count":0,"queues":[]}
```

The OpenAPI is at `https://mq.connected-cloud.io/openapi.yaml` and is authoritative for paths and shapes.

## Naming rules

| Resource     | Rule (observed)                              |
|--------------|----------------------------------------------|
| Queue name   | `[A-Za-z0-9_-]{1,64}` — **no dots**          |
| Topic name   | Same rule — **no dots**                      |
| Subject      | Dots allowed; this is the NATS hierarchy.    |

Implication for this project: queue/topic names use `_` separators (`inference_req_<model>`, `inference_responses`, `worker_presence`). Subjects retain dots (`inference.<corr_id>.token`, `presence.<wkr_id>.<model>`).

The full NATS subject the substrate stores becomes `t.<tenant_id>__<topic>.<subject>` — clients see only the trailing portion in subscription frames.

## Endpoints + shapes (authoritative, observed)

### Queues

| Op       | Method/path                                    | Notes                                          |
|----------|------------------------------------------------|------------------------------------------------|
| Create   | `POST /queues`                                 | 201; `{name, replicas?, max_age?}`. **Idempotent — duplicate returns 201, not 409**. |
| Send     | `POST /queues/{q}/messages`                    | `Content-Type: application/octet-stream`; body raw bytes. 200 → `{queue, msg_id, stream, duplicate}`. |
| Receive  | `GET  /queues/{q}/messages?wait=N`             | 200 → `{count, messages:[{ack_token, body_b64, msg_id, delivery_count, subject, timestamp}]}`. Empty: `count:0, messages:[]` (NOT 204). **Max wait 20s** (`wait` is capped server-side). |
| Ack      | `DELETE /queues/{q}/messages/{ack_token}`      | 204.                                           |
| Nak      | `POST /queues/{q}/messages/{ack_token}/nak?delay=10s` |                                         |
| Extend   | `POST /queues/{q}/messages/{ack_token}/extend` |                                                |
| Delete   | `DELETE /queues/{q}`                           | 204.                                           |

Queue defaults observed at create time: `ack_wait: 25s`, `max_deliver: 5`, `max_age: 24h0m0s`, `replicas: 3`. **A message left unacked for >25s is redelivered**; after 5 unacked deliveries it goes to DLQ.

### Topics

| Op       | Method/path                                              | Notes                                                          |
|----------|----------------------------------------------------------|----------------------------------------------------------------|
| Create   | `POST /topics`                                           | 201; same name rule as queues; idempotent.                     |
| Publish  | `POST /topics/{t}/publish` + `X-MQ-Subject: <subject>`   | Body `application/json`. 200 → `{topic, subject, msg_id, stream, duplicate}`. Subject may contain dots. |
| Publish  | `POST /topics/{t}/publish/{subjectSuffix}`               | Subject in path is an alternative to the header.               |
| Delete   | `DELETE /topics/{t}`                                     | 204.                                                           |

### Subscriptions

| Op       | Method/path                                              | Notes                                                          |
|----------|----------------------------------------------------------|----------------------------------------------------------------|
| Create   | `POST /subscriptions`                                    | `{topic, filter?, delivery:{type, url?}}`. 201 → `{id, consumer, topic, filter, delivery}`. |
| Stream (SSE) | `GET /subscriptions/{id}/stream?bearer=<token>`      | `?bearer=` is accepted (OpenAPI says `?session=` for signed tokens; bearer works fine in practice). |
| Delete   | `DELETE /subscriptions/{id}`                             | 204.                                                           |

Subscription create normalizes the `filter` field to a substrate-prefixed form (`t.<tenant>__<topic>.<your-filter>`) in the response, but you pass it as the leaf form.

### SSE event format

```
: connected sub=<id> topic=<full> filter=<full>

id: 3
event: message
data: {"body_b64":"<base64>","msg_id":3,"subject":"<full-prefixed-subject>","timestamp":"..."}

```

The `subject` field on each frame includes the substrate prefix (`t.<tenant>__<topic>.<your-subject>`). To recover the application-level subject (e.g. `.token` vs `.done`), strip the `t.<tenant>__<topic>.` prefix — or just look at the last dotted segment.

### http_push delivery

When you set `delivery: {type: http_push, url: <your-url>}`, the substrate POSTs each matching message to your URL with:

- **Body**: raw bytes of the originally published payload (no envelope).
- **Content-Type**: `application/octet-stream`.
- **Headers**:
  - `x-mq-subject` — full substrate-prefixed subject
  - `x-mq-topic`   — full substrate-prefixed topic name
  - `x-mq-id`      — substrate `msg_id`
  - `x-mq-attempt` — delivery attempt count (1, 2, 3 on retries)
  - `x-mq-timestamp` — ms since epoch

Implication for the router's `/v1/internal/presence` endpoint: deserialize the body directly as the heartbeat JSON. No envelope-unwrapping needed.

## Behavioral confirmations

| Question | Answer |
|---|---|
| Does long-poll `GET messages?wait=20` actually wait 20s on empty queue? | **Yes** — measured 20.28s wall-clock. |
| Does duplicate queue/topic create return 409? | **No** — 201, idempotent. |
| Does `?bearer=` work on the SSE stream URL? | **Yes**, despite OpenAPI documenting `?session=<signed-token>`. |
| Do SSE subscriptions buffer messages published *before* the client connects? | **Yes** — a message published 2s before the SSE GET was delivered on connect. The "streaming race" originally flagged in `architecture.md` is moot. |
| Do dotted subjects work even though dotted topic *names* are rejected? | **Yes** — `inference.corr_xyz.token` round-trips fine through topic publish and subscription filtering. |
| Is the http_push body wrapped in any envelope? | **No** — raw published bytes. |

## What still hasn't been validated live

These remain unprobed; they shouldn't block v0.1 but are worth nailing before any prod-style deploy:

- Whether deleting a queue while a receiver is long-polling races (e.g. router's clean-up DELETE during a worker `wait=20`).
- Whether DLQ behavior triggers as documented for our queue defaults.
- Whether a subscription survives the SSE client momentarily disconnecting (reconnect-and-replay).
- End-to-end test of the http_push path from the substrate to a router running on FWF (we've only tested webhook.site as the receiver).
- Behavior of the substrate when the http_push target returns 5xx or times out (substrate-side retry policy → `x-mq-attempt` increments).
- Whether the SSE stream sends keepalive comments during idle periods, and what `EventSource.onerror` fires look like.

## Open SCOPE/deploy questions (not substrate-level)

These were in SCOPE.md and are unchanged by validation:

- Own Akamai property + cert or piggyback on existing `connected-cloud.io` infra?
- Public-facing demo vs internal-only?
- FWF deployment target — does the Spin default KV store exist in that environment?
