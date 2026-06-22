#!/usr/bin/env bash
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR" || exit 1

if command -v podman-compose >/dev/null 2>&1; then
  COMPOSE_CMD=(podman-compose)
elif command -v podman >/dev/null 2>&1 && podman compose version >/dev/null 2>&1; then
  COMPOSE_CMD=(podman compose)
elif command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD=(docker-compose)
else
  echo "missing Compose. Install Docker Compose, podman-compose, or the Podman compose plugin." >&2
  exit 1
fi

compose() {
  "${COMPOSE_CMD[@]}" "$@"
}

timestamp="$(date +%Y%m%d-%H%M%S)"
OUT_DIR="${1:-outputs/rubric-evidence-${timestamp}}"
mkdir -p "$OUT_DIR"

run_capture() {
  local name="$1"
  local description="$2"
  shift 2

  local file="${OUT_DIR}/${name}.txt"
  {
    echo "# ${description}"
    echo
    printf '$'
    printf ' %q' "$@"
    echo
    echo
  } >"$file"

  "$@" >>"$file" 2>&1
  local status=$?
  {
    echo
    echo "# exit status: ${status}"
  } >>"$file"

  if [ "$status" -eq 0 ]; then
    echo "ok: ${file}"
  else
    echo "saved with non-zero status (${status}): ${file}"
  fi
}

run_shell() {
  local name="$1"
  local description="$2"
  local command="$3"

  local file="${OUT_DIR}/${name}.txt"
  {
    echo "# ${description}"
    echo
    echo "$ ${command}"
    echo
  } >"$file"

  eval "$command" >>"$file" 2>&1
  local status=$?
  {
    echo
    echo "# exit status: ${status}"
  } >>"$file"

  if [ "$status" -eq 0 ]; then
    echo "ok: ${file}"
  else
    echo "saved with non-zero status (${status}): ${file}"
  fi
}

json_get() {
  local url="$1"
  curl -fsS "$url" | python -m json.tool
}

ksql_post() {
  local statement="$1"
  curl -fsS \
    -X POST \
    -H "Content-Type: application/vnd.ksql.v1+json" \
    --data "{\"ksql\":\"${statement}\",\"streamsProperties\":{}}" \
    http://localhost:8088/ksql | python -m json.tool
}

first_topic_matching() {
  local pattern="$1"
  compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list \
    | grep -E "$pattern" \
    | sort \
    | head -n 1
}

consume_topic() {
  local topic="$1"
  local max_messages="${2:-5}"
  local timeout_seconds="${3:-20}"

  compose exec -T kafka sh -lc \
    "timeout ${timeout_seconds} kafka-console-consumer --bootstrap-server kafka:29092 --topic '${topic}' --from-beginning --max-messages ${max_messages}"
}

cat >"${OUT_DIR}/README.txt" <<EOF
Rubric evidence captured at ${timestamp}

Run this after the container stack, simulation, Faust worker, and UI server have
been running long enough to produce data.

Compose command: ${COMPOSE_CMD[*]}
Output folder: ${OUT_DIR}
EOF

run_capture "00-compose-services" "Compose service status" compose ps
run_capture "01-kafka-topics-all" "All Kafka topics" compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list
run_shell "02-kafka-topics-rubric" "Rubric-related Kafka topics" \
  "compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list | sort | grep -E 'org\\.chicago\\.cta\\.(station\\.arrivals|station\\.turnstile|weather|stations)|TURNSTILE|stations\\.table'"
run_shell "03-arrival-topic-counts" "Arrival topics by train line" \
  "compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list | awk '/org\\.chicago\\.cta\\.station\\.arrivals\\.blue\\./ {blue++} /org\\.chicago\\.cta\\.station\\.arrivals\\.green\\./ {green++} /org\\.chicago\\.cta\\.station\\.arrivals\\.red\\./ {red++} END {print \"blue=\" blue+0; print \"green=\" green+0; print \"red=\" red+0}'"

arrival_topic="$(first_topic_matching '^org\.chicago\.cta\.station\.arrivals\.(blue|green|red)\.')"
if [ -n "$arrival_topic" ]; then
  run_shell "04-sample-arrival-messages" "Sample Avro arrival messages from ${arrival_topic}" \
    "consume_topic '${arrival_topic}' 5 20"
else
  echo "no arrival topic found" >"${OUT_DIR}/04-sample-arrival-messages.txt"
fi

run_shell "05-sample-turnstile-messages" "Sample Avro turnstile messages" \
  "consume_topic 'org.chicago.cta.station.turnstile.v1' 5 20"
run_shell "06-sample-weather-messages" "Sample weather messages produced through Kafka REST Proxy" \
  "consume_topic 'org.chicago.cta.weather.v1' 5 20"
run_shell "07-sample-stations-messages" "Sample station records loaded by Kafka Connect" \
  "consume_topic 'org.chicago.cta.stations' 10 20"
run_shell "08-sample-transformed-stations" "Sample transformed station records produced by Faust" \
  "consume_topic 'org.chicago.cta.stations.table.v1' 10 20"
run_shell "09-sample-turnstile-summary" "Sample KSQL turnstile summary records" \
  "consume_topic 'TURNSTILE_SUMMARY' 10 20"

run_shell "10-schema-registry-subjects" "All Schema Registry subjects" \
  "json_get http://localhost:8081/subjects"
run_shell "11-arrival-schema-subjects" "Arrival schema subjects" \
  "curl -fsS http://localhost:8081/subjects | python -m json.tool | grep 'station.arrivals' || true"
run_shell "12-turnstile-schema" "Turnstile value schema" \
  "json_get http://localhost:8081/subjects/org.chicago.cta.station.turnstile.v1-value/versions/latest"
run_shell "13-weather-schema" "Weather value schema" \
  "json_get http://localhost:8081/subjects/org.chicago.cta.weather.v1-value/versions/latest"
run_shell "14-stations-key-schema" "Kafka Connect stations key schema" \
  "json_get http://localhost:8081/subjects/org.chicago.cta.stations-key/versions/latest"
run_shell "15-stations-value-schema" "Kafka Connect stations value schema" \
  "json_get http://localhost:8081/subjects/org.chicago.cta.stations-value/versions/latest"

run_shell "16-connect-config" "Kafka Connect stations connector configuration" \
  "json_get http://localhost:8083/connectors/stations/config"
run_shell "17-connect-status" "Kafka Connect stations connector status" \
  "json_get http://localhost:8083/connectors/stations/status"
run_capture "18-postgres-station-count" "Station rows in Postgres" compose exec -T postgres psql -U cta_admin -d cta -c "SELECT COUNT(*) FROM stations;"

run_capture "19-consumer-groups" "Kafka consumer groups, including Faust" compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 --list
run_shell "20-faust-consumer-group" "Faust consumer group on stations topic" \
  "compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group stations-stream 2>/dev/null || compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 --list | grep -i faust"

run_shell "21-ksql-streams-tables-queries" "KSQL streams, tables, and queries" \
  "ksql_post 'SHOW STREAMS; SHOW TABLES; SHOW QUERIES;'"
run_shell "22-ksql-describe-turnstile" "KSQL TURNSTILE table description" \
  "ksql_post 'DESCRIBE TURNSTILE;'"
run_shell "23-ksql-describe-turnstile-summary" "KSQL TURNSTILE_SUMMARY table description" \
  "ksql_post 'DESCRIBE TURNSTILE_SUMMARY;'"
run_shell "24-ksql-select-turnstile-summary" "KSQL turnstile summary rows with station IDs and counts" \
  "compose exec -T ksql-server ksql http://ksql-server:8088 --execute 'SELECT * FROM TURNSTILE_SUMMARY LIMIT 10;'"

run_shell "25-ui-http" "Transit Status UI HTTP response" \
  "curl -fsS -D - -o /dev/null http://localhost:3000/"
run_shell "26-ui-html-snapshot" "Transit Status UI HTML snapshot" \
  "curl -fsS http://localhost:3000/"

cat <<EOF

Evidence collection complete.
Output folder: ${OUT_DIR}

Files with non-zero exit status usually mean the related service or producer had
not produced data yet. Start the stack, simulation, Faust, and UI, wait a minute,
then rerun this script.
EOF
