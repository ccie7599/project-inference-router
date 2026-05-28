#!/usr/bin/env bash
# End-to-end smoke test for the router.
#
# Required env / args:
#   ROUTER_URL — base URL of the router (default http://localhost:3000)
#   MODEL      — model to ask for (default llama3.2:1b)

set -euo pipefail

ROUTER_URL="${ROUTER_URL:-http://localhost:3000}"
MODEL="${MODEL:-llama3.2:1b}"
PROMPT="${PROMPT:-In one sentence, why is the sky blue?}"

echo "==> /healthz"
curl -sS "$ROUTER_URL/healthz"

echo "==> req/res (stream: false)"
curl -sS -X POST "$ROUTER_URL/v1/inference" \
  -H 'Content-Type: application/json' \
  -d "{\"model\":\"$MODEL\",\"prompt\":\"$PROMPT\",\"stream\":false}" \
  | tee /tmp/smoke-reqres.json
echo

echo "==> streaming (stream: true) — following 302 to substrate SSE"
# -L follows the 302 to the substrate SSE; --max-time caps total wall-clock.
# We pipe through head so we exit once we see some tokens + done.
curl -sS -N -L -X POST "$ROUTER_URL/v1/inference" \
  -H 'Content-Type: application/json' \
  -d "{\"model\":\"$MODEL\",\"prompt\":\"$PROMPT\",\"stream\":true}" \
  --max-time 30 \
  | sed -u 's/^/  /' \
  | awk '/inference\..*\.done/ { print; exit } { print }'
echo
echo "==> smoke complete"
