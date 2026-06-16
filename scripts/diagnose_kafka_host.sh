#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if command -v podman-compose >/dev/null 2>&1; then
  COMPOSE_CMD=(podman-compose)
elif command -v podman >/dev/null 2>&1 && podman compose version >/dev/null 2>&1; then
  COMPOSE_CMD=(podman compose)
elif command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD=(docker-compose)
else
  echo "No compose command found."
  exit 1
fi

compose() {
  "${COMPOSE_CMD[@]}" "$@"
}

echo "compose command: ${COMPOSE_CMD[*]}"
echo

echo "shell KAFKA_BOOTSTRAP_SERVERS: ${KAFKA_BOOTSTRAP_SERVERS:-<unset>}"
echo "expected for host Python: 127.0.0.1:9092"
echo

echo "compose service status:"
compose ps
echo

if command -v podman >/dev/null 2>&1 && podman container exists cta-kafka; then
  echo "podman port cta-kafka:"
  podman port cta-kafka || true
  echo
fi

if command -v docker >/dev/null 2>&1 && docker container inspect cta-kafka >/dev/null 2>&1; then
  echo "docker port cta-kafka:"
  docker port cta-kafka || true
  echo
fi

echo "host TCP check for 127.0.0.1:9092:"
if nc -vz 127.0.0.1 9092 >/dev/null 2>&1; then
  echo "ok: host can connect to 127.0.0.1:9092"
else
  echo "failed: host cannot connect to 127.0.0.1:9092"
fi
echo

echo "kafka in-container topic check:"
if compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list >/dev/null; then
  echo "ok: kafka responds inside compose network"
else
  echo "failed: kafka does not respond inside compose network"
fi
