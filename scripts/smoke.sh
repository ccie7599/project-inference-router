#!/usr/bin/env bash
# End-to-end smoke test for the router.
# Defaults to the public Akamai URL; override for local / direct-to-FWF.
#
# Env:
#   ROUTER_URL — base URL of the router (default https://inference.connected-cloud.io)
#   MODEL      — model to ask for (default llama3.2:1b)
#   PROMPT     — prompt to send (default short canned prompt)

set -euo pipefail

ROUTER_URL="${ROUTER_URL:-https://inference.connected-cloud.io}"
MODEL="${MODEL:-llama3.2:1b}"
PROMPT="${PROMPT:-In one sentence, why is the sky blue?}"

fail() { echo "FAIL: $*" >&2; exit 1; }

echo "==> ROUTER_URL=$ROUTER_URL MODEL=$MODEL"

echo "==> /healthz"
out=$(curl -sS -o /tmp/healthz.out -w '%{http_code}|%{time_total}' "$ROUTER_URL/healthz" --max-time 10)
code=${out%%|*}; t=${out##*|}
echo "    HTTP $code in ${t}s — $(cat /tmp/healthz.out)"
[[ "$code" == "200" ]] || fail "/healthz returned $code"

echo "==> req/res (stream: false)"
out=$(curl -sS -o /tmp/reqres.json -w '%{http_code}|%{time_total}' --max-time 45 \
  -X POST "$ROUTER_URL/v1/inference" \
  -H 'Content-Type: application/json' \
  -d "{\"model\":\"$MODEL\",\"prompt\":\"$PROMPT\",\"stream\":false}")
code=${out%%|*}; t=${out##*|}
echo "    HTTP $code in ${t}s"
[[ "$code" == "200" ]] || fail "req/res returned $code (body: $(cat /tmp/reqres.json))"
jq -e '.response and .corr_id and .tokens' /tmp/reqres.json >/dev/null || fail "req/res response missing fields: $(cat /tmp/reqres.json)"
echo "    response: $(jq -r '.response' /tmp/reqres.json)"
echo "    tokens: $(jq -r '.tokens' /tmp/reqres.json)"

echo "==> streaming (stream: true) — following 302, reading SSE until .done"
out=$(curl -sS -o /tmp/stream.out -D /tmp/stream.hdr -w '%{http_code}' --max-time 5 \
  -X POST "$ROUTER_URL/v1/inference" \
  -H 'Content-Type: application/json' \
  -d "{\"model\":\"$MODEL\",\"prompt\":\"$PROMPT\",\"stream\":true}")
echo "    POST HTTP $out (expect 302)"
[[ "$out" == "302" ]] || fail "streaming POST returned $out (expected 302)"
loc=$(grep -i '^location:' /tmp/stream.hdr | sed 's/^[Ll]ocation: //; s/\r//')
[[ -n "$loc" ]] || fail "no Location header"
echo "    Location: ${loc:0:80}..."

token_count=0
done_seen=0
while IFS= read -r line; do
  if [[ "$line" =~ inference\..*\.token ]]; then
    token_count=$((token_count + 1))
  fi
  if [[ "$line" =~ inference\..*\.done ]]; then
    done_seen=1
    break
  fi
done < <(timeout 45 curl -sS -N "$loc" 2>/dev/null)
echo "    tokens streamed: $token_count, done event: $done_seen"
[[ "$done_seen" == "1" ]] || fail "streaming did not reach .done within 45s"
[[ "$token_count" -gt 0 ]] || fail "no token events received"

echo "==> smoke PASS"
