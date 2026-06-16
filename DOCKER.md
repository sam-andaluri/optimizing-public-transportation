# Local Docker and Podman Stack

This file keeps the container runbook details for local development. The main project overview is in [README.md](README.md).

This Compose stack runs the infrastructure needed by the project:

- Zookeeper
- Kafka
- Schema Registry
- Kafka REST Proxy
- Kafka Connect with the JDBC connector and PostgreSQL driver
- KSQL server
- Postgres seeded with `startup/cta_stations.csv`

Start the stack:

```bash
docker compose up -d --build
```

The Compose file defaults to `linux/amd64`, which is the safest choice for the Confluent images on Mac and Ubuntu. Override it only if your Docker installation has compatible native images:

```bash
DOCKER_PLATFORM=linux/arm64 docker compose up -d --build
```

On older Linux installs, use `docker-compose` instead:

```bash
docker-compose up -d --build
```

Verify the stack before running project code:

```bash
./scripts/verify_docker_stack.sh
```

The verifier checks:

- HTTP endpoints for Schema Registry, REST Proxy, Kafka Connect, and KSQL
- Postgres readiness and seeded station row count
- Kafka topic admin path
- REST Proxy produce path
- Kafka Connect JDBC source connector plugin availability
- KSQL request path

## Python Environment

Use Python 3.7 or 3.8 for the project code. The legacy pinned dependencies are not expected to install cleanly on modern Python versions such as Python 3.12 or 3.13.

Create and activate a virtual environment:

```bash
cd /Users/sandalur/projects/udacity-data-streaming/project-2
python3.8 -m venv .venv
source .venv/bin/activate
```

Upgrade packaging tools:

```bash
python -m pip install --upgrade pip setuptools wheel
```

Install producer and consumer dependencies.

For local macOS/Linux development, use the local requirement files:

```bash
python -m pip install -r producers/requirements-local.txt
python -m pip install -r consumers/requirements-local.txt
```

The original pinned requirement files are also available:

```bash
python -m pip install -r producers/requirements.txt
python -m pip install -r consumers/requirements.txt
```

Check that the key packages import:

```bash
python -c "import confluent_kafka, faust, pandas, tornado, requests; print('python dependencies ok')"
```

On Apple Silicon, install the `requirements-local.txt` files first. If a wheel still fails to install, use an x86_64 Python 3.8 environment under Rosetta or run the project app code inside an amd64 Linux container. The infrastructure containers already default to `linux/amd64`.

If a Python Kafka client logs `Failed to resolve 'localhost:9092'`, make sure `KAFKA_BOOTSTRAP_SERVERS` is not set with a protocol prefix. On Mac/Podman, use the IPv4 loopback address:

```bash
export KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
```

Do not use `PLAINTEXT://localhost:9092` for Python clients.

After preflight passes, run the project apps from separate host terminals:

```bash
python producers/simulation.py
```

```bash
faust -A consumers.faust_stream worker -l info
```

```bash
python consumers/server.py
```

Then open `http://localhost:3000`.

The code defaults are set for services running on the local host. If you run against this Docker/Podman Compose infrastructure, set:

```bash
export POSTGRES_HOST=postgres
```

## Cleanup

Stop the stack but keep Kafka and Postgres data:

```bash
docker compose down
```

For Podman Compose:

```bash
podman-compose down
```

Reset all Kafka/Postgres state and reseed from scratch:

```bash
docker compose down -v
docker compose up -d --build
./scripts/verify_docker_stack.sh
```

For Podman Compose:

```bash
podman-compose down -v
podman-compose up -d --build
./scripts/verify_docker_stack.sh
```

Remove the custom Kafka Connect image after stopping the stack:

```bash
docker image rm project-2-connect
```

For Podman, first find the image name:

```bash
podman images | grep connect
podman image rm <connect-image-name>
```

Remove dangling build cache and unused local images:

```bash
docker system prune
```

For Podman:

```bash
podman system prune
```

Remove a dedicated Podman machine only when you no longer need it:

```bash
podman machine stop cta-kafka
podman machine rm cta-kafka
```

Do not remove `podman-machine-default` unless you are sure no other local Podman work depends on it.
