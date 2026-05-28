#!/usr/bin/env bash
# Creates the substrate resources the router + worker depend on.
# Idempotent: 409 (already exists) is treated as success.
#
# Required env / args:
#   ROUTER_PRESENCE_URL — the URL the substrate should POST heartbeats to.
#                        For local dev:   http://host.docker.internal:3000/v1/internal/presence
#                        For prod:        https://inference.connected-cloud.io/v1/internal/presence
#
# Optional:
#   MODEL              — model name (default llama3.2:1b). Used to create the work queue.
#   BEARER_FILE        — path to bearer (default .tenant-bearer)
#   SUBSTRATE_BASE     — substrate base URL (default https://mq.connected-cloud.io/v1)

set -euo pipefail

MODEL="${MODEL:-llama3.2:1b}"
BEARER_FILE="${BEARER_FILE:-$(dirname "$0")/../.tenant-bearer}"
SUBSTRATE_BASE="${SUBSTRATE_BASE:-https://mq.connected-cloud.io/v1}"
ROUTER_PRESENCE_URL="${ROUTER_PRESENCE_URL:-}"

if [[ -z "$ROUTER_PRESENCE_URL" ]]; then
  echo "ROUTER_PRESENCE_URL must be set (the URL the substrate should POST heartbeats to)." >&2
  exit 1
fi

if [[ ! -f "$BEARER_FILE" ]]; then
  echo "bearer file not found: $BEARER_FILE" >&2
  exit 1
fi

BEARER="$(tr -d '[:space:]' < "$BEARER_FILE")"

# Lowercase + replace : . / space with -
normalize() {
  printf '%s' "$1" | tr ':./ ' '----'
}

NORMALIZED_MODEL="$(normalize "$MODEL")"
REQ_QUEUE="inference.${NORMALIZED_MODEL}.req"

# create takes a path (/queues, /topics) and a resource name; treats 200/201/409 as success.
create() {
  local path="$1" name="$2"
  local code
  code="$(curl -sS -o /tmp/bootstrap.out -w '%{http_code}' \
    -X POST "$SUBSTRATE_BASE$path" \
    -H "Authorization: Bearer $BEARER" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"$name\"}")"
  case "$code" in
    2*|409) echo "  ${path}  $name  -> $code" ;;
    *) echo "  ${path}  $name  -> $code"; cat /tmp/bootstrap.out; echo; exit 1 ;;
  esac
}

echo "==> creating queues"
create /queues "$REQ_QUEUE"

echo "==> creating topics"
create /topics "inference.responses"
create /topics "worker.presence"

echo "==> creating presence-collector subscription"
# Subscription POST shape per docs/using-nats-mq.md:
#   {"topic":..., "filter":..., "delivery":{"type":"http_push","url":...}}
SUB_PAYLOAD="$(cat <<JSON
{
  "topic": "worker.presence",
  "filter": "worker.>",
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
elif [[ "$code" == "409" ]]; then
  echo "  subscription already exists"
else
  echo "  subscription create failed ($code):"
  cat /tmp/sub.out
  exit 1
fi

echo "==> bootstrap complete"
echo "    request queue: $REQ_QUEUE"
echo "    presence URL:  $ROUTER_PRESENCE_URL"
