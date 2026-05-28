#!/usr/bin/env bash
# Creates the substrate resources the router + worker depend on.
# Idempotent: substrate returns 201 for duplicate create.
#
# Required env / args:
#   ROUTER_PRESENCE_URL — URL the substrate should POST heartbeats to.
#                        Local dev: https://<tunnel-host>/v1/internal/presence
#                        Prod:      https://inference.connected-cloud.io/v1/internal/presence
#
# Optional:
#   MODEL              — model name (default llama3.2:1b). Used to derive the work queue name.
#   BEARER_FILE        — path to bearer (default .tenant-bearer)
#   SUBSTRATE_BASE     — substrate base URL (default https://mq.connected-cloud.io/v1)

set -euo pipefail

MODEL="${MODEL:-llama3.2:1b}"
BEARER_FILE="${BEARER_FILE:-$(dirname "$0")/../.tenant-bearer}"
SUBSTRATE_BASE="${SUBSTRATE_BASE:-https://mq.connected-cloud.io/v1}"
ROUTER_PRESENCE_URL="${ROUTER_PRESENCE_URL:-}"

if [[ -z "$ROUTER_PRESENCE_URL" ]]; then
  echo "ROUTER_PRESENCE_URL must be set (URL the substrate POSTs heartbeats to)." >&2
  exit 1
fi
if [[ ! -f "$BEARER_FILE" ]]; then
  echo "bearer file not found: $BEARER_FILE" >&2
  exit 1
fi

BEARER="$(tr -d '[:space:]' < "$BEARER_FILE")"

# Substrate name rule: [A-Za-z0-9_-]{1,64}. Replace anything else with `-`.
normalize() { printf '%s' "$1" | tr -c 'A-Za-z0-9_-' '-' | cut -c1-64; }

NORMALIZED_MODEL="$(normalize "$MODEL")"
REQ_QUEUE="inference_req_${NORMALIZED_MODEL}"
RESPONSE_TOPIC="inference_responses"
PRESENCE_TOPIC="worker_presence"

create() {
  local path="$1" name="$2"
  local code
  code="$(curl -sS -o /tmp/bootstrap.out -w '%{http_code}' \
    -X POST "$SUBSTRATE_BASE$path" \
    -H "Authorization: Bearer $BEARER" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"$name\"}")"
  if [[ "$code" =~ ^2 ]]; then
    echo "  ${path}  $name  -> $code"
  else
    echo "  ${path}  $name  -> $code"
    cat /tmp/bootstrap.out; echo; exit 1
  fi
}

echo "==> creating queues"
create /queues "$REQ_QUEUE"

echo "==> creating topics"
create /topics "$RESPONSE_TOPIC"
create /topics "$PRESENCE_TOPIC"

echo "==> creating presence-collector subscription (http_push -> router)"
SUB_PAYLOAD="$(cat <<JSON
{
  "topic": "$PRESENCE_TOPIC",
  "filter": "presence.>",
  "delivery": {"type": "http_push", "url": "$ROUTER_PRESENCE_URL"}
}
JSON
)"
code="$(curl -sS -o /tmp/sub.out -w '%{http_code}' \
  -X POST "$SUBSTRATE_BASE/subscriptions" \
  -H "Authorization: Bearer $BEARER" \
  -H 'Content-Type: application/json' \
  -d "$SUB_PAYLOAD")"
if [[ "$code" =~ ^2 ]]; then
  echo "  subscription created: $(cat /tmp/sub.out)"
else
  echo "  subscription create failed ($code):"
  cat /tmp/sub.out
  exit 1
fi

echo "==> bootstrap complete"
echo "    request queue:  $REQ_QUEUE"
echo "    response topic: $RESPONSE_TOPIC"
echo "    presence topic: $PRESENCE_TOPIC"
echo "    presence URL:   $ROUTER_PRESENCE_URL"
