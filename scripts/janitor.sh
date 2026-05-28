#!/usr/bin/env bash
# janitor.sh — reap orphan response queues left behind by the router.
#
# The router (Spin function on FWF) doesn't delete its per-corr-id response
# queues because Spin SDK's outbound DELETE hangs in the FWF runtime. See
# DECISIONS.md ADR-004.
#
# This script lists all queues, finds any matching `inference_resp_*`,
# and deletes them. Run periodically from any host with the bearer.
#
# Suggested cadence: every 5–10 min (cron / systemd timer). With the default
# 50-queue tenant quota, this keeps the active set under control even at
# moderate request rates.
#
# Env:
#   BEARER_FILE      path to bearer (default .tenant-bearer)
#   SUBSTRATE_BASE   default https://mq.connected-cloud.io/v1
#   DRY_RUN          if set to "1", just list, don't delete

set -euo pipefail

BEARER_FILE="${BEARER_FILE:-$(dirname "$0")/../.tenant-bearer}"
SUBSTRATE_BASE="${SUBSTRATE_BASE:-https://mq.connected-cloud.io/v1}"
DRY_RUN="${DRY_RUN:-0}"

if [[ ! -f "$BEARER_FILE" ]]; then
  echo "bearer file not found: $BEARER_FILE" >&2; exit 1
fi
BEARER="$(tr -d '[:space:]' < "$BEARER_FILE")"

resp_queues=$(curl -sS -H "Authorization: Bearer $BEARER" "$SUBSTRATE_BASE/queues" \
  | jq -r '.queues[]?.queue | select(startswith("inference_resp_"))')

if [[ -z "$resp_queues" ]]; then
  echo "no orphan response queues"; exit 0
fi

count=0
while IFS= read -r q; do
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "  would delete: $q"
  else
    code=$(curl -sS -o /dev/null -w '%{http_code}' -X DELETE "$SUBSTRATE_BASE/queues/$q" \
      -H "Authorization: Bearer $BEARER")
    echo "  delete $q -> $code"
  fi
  count=$((count + 1))
done <<< "$resp_queues"
echo "==> $count queue(s) processed"
