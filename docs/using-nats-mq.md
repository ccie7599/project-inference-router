# Using the Akamai Cloud Queues + Notifications service

This project consumes `nats-mq` (`https://mq.connected-cloud.io`) as its messaging fabric. A dedicated tenant has been provisioned. Use the bearer in `.tenant-bearer` (at repo root, gitignored) on every API call.

## Identity

- **Tenant ID**: `t_daed309425e205a86372063a8bcd7aa8`
- **Tag**: `inference-router`
- **Bearer**: see `.tenant-bearer` at the repo root
- **Quotas**: 50 queues, 50 topics, 100 subs, 5 GiB storage (defaults — bump via control plane if needed)

## Endpoints (data plane)

Base: `https://mq.connected-cloud.io/v1`

| Path | Purpose |
|---|---|
| `POST /queues` | Create a queue (`{"name":"..."}`) |
| `GET  /queues` | List your queues |
| `DELETE /queues/<n>` | Delete a queue |
| `POST /queues/<n>/send` | Publish to a queue (body = message body) |
| `GET  /queues/<n>/receive?wait=30` | Long-poll receive one message (returns `ack_token`) |
| `POST /queues/<n>/ack/<token>` | Ack a message |
| `POST /queues/<n>/nak/<token>` | Negative-ack (redeliver immediately) |
| `POST /queues/<n>/extend/<token>` | Extend the visibility window |
| `POST /topics` | Create a topic |
| `POST /topics/<n>/publish` | Publish to a topic (`X-MQ-Subject: <sub>` header carries subject) |
| `POST /subscriptions` | Create a subscription (`{"topic":..., "filter":..., "delivery":{"type":"sse"|"http_push","url":...}}`) |
| `GET  /subscriptions` | List your subscriptions |
| `DELETE /subscriptions/<id>` | Delete a sub |
| `GET  /subscriptions/<id>/stream` | SSE stream (long-lived) |

CORS is wide-open (`Access-Control-Allow-Origin: *`) so a browser at any origin can hit these directly.

## Auth shape

- **Authorization header** for everything except browser EventSource: `Authorization: Bearer <token>`
- **EventSource fallback** (browsers can't set headers on EventSource): pass the bearer in the query string: `?bearer=<token>`. Same KeyCache validation.

## Quick verification

```bash
BEARER=$(cat ~/project-inference-router/.tenant-bearer)
BASE=https://mq.connected-cloud.io/v1

# this should list your queues (empty array first time)
curl -s -H "Authorization: Bearer $BEARER" $BASE/queues
```

## Worker-side: connect via NATS native protocol (not HTTP)

The inference workers in this design talk directly to NATS — not through `mq.connected-cloud.io`. That gives queue-group load balancing, lower per-message overhead, and access to JetStream + KV primitives the HTTP API doesn't expose (`micro` framework registration, presence buckets with TTL, durable consumer ergonomics).

The HTTP API at `mq.connected-cloud.io` and the NATS native endpoint share the same JetStream substrate and the same tenant scoping. A topic created via HTTP is the same stream a NATS-direct subscriber sees.

NATS native endpoint: not exposed publicly today. For local development, run a NATS sidecar that connects to the cluster, or coordinate with the nats-mq maintainer (Brian) for direct-cluster credentials. For the v0.1 demo, all-HTTP via the router is acceptable; native NATS for workers can come in v0.2 when the public NKey path is opened.

## Router-side: how to do the streaming-tokens response shape

The pattern (from `docs/concept.md`):

1. Client `POST`s to your router's request URL (something behind FWF)
2. Router function:
   - Creates a fresh correlation ID `corr_<uuid>`
   - Creates a topic `inference.<corr_id>.tokens` (or pre-creates a single shared topic with filter subject `inference.<corr_id>.*`)
   - Creates a subscription with `delivery.type: sse` on that topic, scoped to filter `inference.<corr_id>.*`
   - Returns 302 to `https://mq.connected-cloud.io/v1/subscriptions/<sub-id>/stream?bearer=<token>`
   - In parallel (background fire-and-forget), publishes the original request to `inference.<model>.req` queue
3. Worker (subscribed to `inference.<model>.req` via NATS queue group) consumes the request, runs inference, publishes each token to `inference.<corr_id>.tokens`
4. Browser's EventSource receives the tokens in real time (the SSE path bypasses FWF wall-clock per Option C routing — connection is held by the LKE adapter, not your function)
5. Worker publishes a final `inference.<corr_id>.done` to close the stream

Cleanup: subscription auto-expires on disconnect; topic can have a short retention if you don't want to keep them around.

## Service docs to read alongside this

- `https://mq.connected-cloud.io/docs` — architecture
- `https://mq.connected-cloud.io/guide` — user guide
- `https://mq.connected-cloud.io/api-explorer` — Swagger UI (Try-It works with your bearer)
- `https://mq.connected-cloud.io/openapi.yaml` — raw OpenAPI

## Admin / regenerate bearer

If you need a new bearer (lost the file, rotation), have an admin call:

```bash
ADMIN_BEARER="<the nats-mq admin bearer>"
curl -X POST -H "Authorization: Bearer $ADMIN_BEARER" \
  -H "Content-Type: application/json" \
  https://mq.connected-cloud.io/api/control/v1/admin/tenants/t_daed309425e205a86372063a8bcd7aa8/keys \
  -d '{"tag":"rotated-YYYY-MM-DD"}'
```

(Or use the admin UI at `https://mq.connected-cloud.io/admin/`.)
