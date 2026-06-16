#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

require_command curl

if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD=(docker-compose)
elif command -v podman >/dev/null 2>&1 && podman compose version >/dev/null 2>&1; then
  COMPOSE_CMD=(podman compose)
elif command -v podman-compose >/dev/null 2>&1; then
  COMPOSE_CMD=(podman-compose)
else
  echo "missing Compose. Install Docker Compose, podman-compose, or the Podman compose plugin." >&2
  exit 1
fi

compose() {
  "${COMPOSE_CMD[@]}" "$@"
}

wait_http() {
  local name="$1"
  local url="$2"
  local attempts="${3:-60}"

  printf 'waiting for %s' "$name"
  for _ in $(seq 1 "$attempts"); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      printf ' ok\n'
      return 0
    fi
    printf '.'
    sleep 2
  done
  printf '\n'
  echo "timed out waiting for $name at $url" >&2
  exit 1
}

wait_postgres() {
  printf 'waiting for postgres'
  for _ in $(seq 1 60); do
    if compose exec -T postgres pg_isready -U cta_admin -d cta >/dev/null 2>&1; then
      printf ' ok\n'
      return 0
    fi
    printf '.'
    sleep 2
  done
  printf '\n'
  echo "timed out waiting for postgres" >&2
  exit 1
}

echo "checking compose services..."
compose ps

wait_postgres
wait_http "schema registry" "http://localhost:8081/subjects"
wait_http "kafka rest proxy" "http://localhost:8082/topics"
wait_http "kafka connect" "http://localhost:8083/connectors"
wait_http "ksql server" "http://localhost:8088/info"

station_count="$(compose exec -T postgres psql -U cta_admin -d cta -tAc 'SELECT COUNT(*) FROM stations;')"
if [ "${station_count}" -lt 1 ]; then
  echo "postgres station seed failed: count=${station_count}" >&2
  exit 1
fi
echo "postgres station rows: ${station_count}"

echo "checking kafka broker..."
compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list >/dev/null
compose exec -T kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create \
  --if-not-exists \
  --topic docker.preflight \
  --partitions 1 \
  --replication-factor 1 >/dev/null

echo "checking rest proxy produce path..."
curl -fsS \
  -X POST \
  -H "Content-Type: application/vnd.kafka.json.v2+json" \
  --data '{"records":[{"key":"preflight","value":{"ok":true}}]}' \
  "http://localhost:8082/topics/docker.preflight" >/dev/null

echo "checking kafka connect jdbc plugin..."
connector_plugins="$(curl -fsS http://localhost:8083/connector-plugins)"
if ! printf '%s' "$connector_plugins" | grep -q "io.confluent.connect.jdbc.JdbcSourceConnector"; then
  echo "JDBC source connector plugin is not loaded in Kafka Connect" >&2
  exit 1
fi

echo "checking ksql request path..."
printf 'waiting for ksql statements'
ksql_ready=0
for _ in $(seq 1 30); do
  if curl -fsS \
    -X POST \
    -H "Content-Type: application/vnd.ksql.v1+json" \
    --data '{"ksql":"SHOW TOPICS;","streamsProperties":{}}' \
    "http://localhost:8088/ksql" >/dev/null 2>&1; then
    printf ' ok\n'
    ksql_ready=1
    break
  fi
  printf '.'
  sleep 2
done

if [ "$ksql_ready" -ne 1 ]; then
  echo "ksql server did not accept SHOW TOPICS" >&2
  exit 1
fi

cat <<'EOF'

Container stack preflight passed.
EOF
