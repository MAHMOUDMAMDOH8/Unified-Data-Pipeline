#!/usr/bin/env sh
set -euo pipefail

: "${SCHEMA_REGISTRY_URL:?SCHEMA_REGISTRY_URL is required}"
SCHEMA_SUBJECT="${SCHEMA_SUBJECT:-online-store-order}"
SCHEMA_PATH="${SCHEMA_PATH:-/schema/order_log_schema.json}"

# Wait for Schema Registry to be ready
printf "Waiting for Schema Registry at %s" "$SCHEMA_REGISTRY_URL"
for i in $(seq 1 60); do
  if curl -sSf "${SCHEMA_REGISTRY_URL}/subjects" >/dev/null 2>&1; then
    echo "\nSchema Registry is up."
    break
  fi
  printf "."
  sleep 1
  if [ "$i" -eq 60 ]; then
    echo "\nERROR: Schema Registry did not become ready in time" >&2
    exit 1
  fi
done

# Read schema file and post
if [ ! -f "$SCHEMA_PATH" ]; then
  echo "ERROR: Schema file not found at $SCHEMA_PATH" >&2
  exit 1
fi

SCHEMA_JSON=$(cat "$SCHEMA_PATH")
PAYLOAD=$(printf '{"schemaType":"JSON","schema":%s}' "$(printf %s "$SCHEMA_JSON" | jq -c -M -r '.')")

RESP=$(curl -sS -X POST \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "$PAYLOAD" \
  "${SCHEMA_REGISTRY_URL%/}/subjects/${SCHEMA_SUBJECT}/versions")

echo "$RESP"
