# Optimizing Public Transportation

This project builds a streaming transit-status pipeline for Chicago CTA train data. It uses Kafka producers, Kafka Connect, Kafka REST Proxy, Faust, KSQL, Postgres, and a Tornado web UI to show station status, train arrivals, turnstile activity, and weather updates.

The project was tested locally on Mac Apple Silicon using Podman with an 8 GiB Podman machine.

## What Runs

- `producers/simulation.py` simulates CTA train arrivals, turnstile entries, and weather.
- Kafka Connect loads station data from Postgres into Kafka.
- Kafka REST Proxy receives weather events.
- Faust transforms station records into the compact station table used by the UI.
- KSQL aggregates turnstile events into station-level counts.
- `consumers/server.py` serves the Transit Status UI at `http://localhost:3000`.

## Runtime Defaults

The Python application defaults are set for services running on the local host:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SCHEMA_REGISTRY_URL=http://localhost:8081
KAFKA_REST_URL=http://localhost:8082
KAFKA_CONNECT_URL=http://localhost:8083/connectors
KSQL_URL=http://localhost:8088
POSTGRES_HOST=localhost
```

For Python Kafka clients, use `host:port` only:

```bash
localhost:9092
```

Do not include the listener protocol in Python client configuration:

```bash
# Do not use this for Python clients:
# PLAINTEXT://localhost:9092
```

## Local Test Environment

The included Compose stack runs:

- Zookeeper
- Kafka
- Schema Registry
- Kafka REST Proxy
- Kafka Connect with the JDBC connector and PostgreSQL driver
- KSQL server
- Postgres seeded with `startup/cta_stations.csv`

Start the stack with Docker Compose:

```bash
docker compose up -d --build
```

Or with Podman Compose:

```bash
podman-compose up -d --build
```

On Mac Apple Silicon with Podman, use at least 6 GiB memory for the Podman machine. 8 GiB is recommended:

```bash
podman machine set --memory 8192 --cpus 5 podman-machine-default
```

The Compose stack advertises Kafka to the host as `127.0.0.1:9092`, which is more reliable on Podman for macOS. When running the Python apps against this Compose stack, set:

```bash
export KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
export FAUST_BROKER_URL=kafka://127.0.0.1:9092
export POSTGRES_HOST=postgres
```

Verify the container stack before running the app:

```bash
./scripts/verify_docker_stack.sh
```

If Kafka connectivity fails, run:

```bash
./scripts/diagnose_kafka_host.sh
```

## Python Environment

Use Python 3.7 or 3.8. The legacy pinned dependencies in `requirements.txt` may not install cleanly on newer Python versions.

Create and activate a virtual environment:

```bash
python3.8 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
```

For local macOS/Linux development, install:

```bash
python -m pip install -r producers/requirements-local.txt
python -m pip install -r consumers/requirements-local.txt
```

The original pinned files are still available:

```bash
python -m pip install -r producers/requirements.txt
python -m pip install -r consumers/requirements.txt
```

Verify imports:

```bash
python -c "import confluent_kafka, faust, pandas, tornado, requests; print('python dependencies ok')"
```

## Run Order

Run each process in a separate terminal. Activate the virtual environment and export the local environment variables in each terminal.

Start the simulation:

```bash
python producers/simulation.py
```

Start Faust:

```bash
faust -A consumers.faust_stream worker -l info
```

Start the UI server:

```bash
python consumers/server.py
```

Open:

```text
http://localhost:3000
```

## Cleanup

Stop containers but keep Kafka/Postgres data:

```bash
docker compose down
```

or:

```bash
podman-compose down
```

Reset Kafka/Postgres state:

```bash
docker compose down -v
docker compose up -d --build
./scripts/verify_docker_stack.sh
```

or:

```bash
podman-compose down -v
podman-compose up -d --build
./scripts/verify_docker_stack.sh
```
