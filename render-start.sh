#!/usr/bin/env bash
set -euo pipefail

mkdir -p \
  /var/data/entertainment \
  /var/data/field/shards \
  /var/data/live_index \
  /var/data/incoming

exec python3 ARBITER_live_field_server.py serve \
  --host 0.0.0.0 \
  --port "${PORT:-10000}" \
  --field-dir /var/data/field \
  --live-dir /var/data/live_index \
  --entertainment-dir /var/data/entertainment \
  --assets-dir /opt/render/project/src \
  --html /opt/render/project/src/index.html \
  --embed-url "${ARBITER_EMBED_URL:-https://api.arbiter.traut.ai/public/embed}" \
  --sync-interval "${FIELD_SYNC_INTERVAL:-20}" \
  --search-chunk-rows "${SEARCH_CHUNK_ROWS:-100000}"
