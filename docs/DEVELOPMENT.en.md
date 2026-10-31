# Development Guide

[한국어](DEVELOPMENT.md) | **English**

**Document Links** | [README](../README.en.md)| [API Document](API.en.md) | [Demo Document](DEMO.en.md)

This document describes how to run Shire Guard locally, manage its dependencies, and test it.

> **Where to Run Commands**
> - Docker Compose commands: `docker/`
> - Rust (cargo) commands: `backend/`
> - Others: repository root
> - Ports and default values are based on `docker/.env.example` (if you changed them, check the actual `.env` values)

## Table of Contents

- [1. Prerequisites](#1-prerequisites)
- [2. Project Initialization](#2-project-initialization)
- [3. Docker Compose](#3-docker-compose)
  - [3.1 Choosing the Message Broker and Cache](#31-choosing-the-message-broker-and-cache)
  - [3.2 Running](#32-running)
  - [3.3 Operation](#33-operation)
  - [3.4 Access Addresses](#34-access-addresses)
  - [3.5 Grafana Setup (Optional)](#35-grafana-setup-optional)
- [4. Running the Server](#4-running-the-server)
- [5. Environment Variables](#5-environment-variables)
  - [`docker/.env`](#dockerenv)
  - [`backend/.env`](#backendenv)
- [6. Commands Used to Add Rust Dependencies](#6-commands-used-to-add-rust-dependencies)
  - [Application Dependencies](#application-dependencies)
  - [Test-only Dependencies](#test-only-dependencies)
- [7. Tests](#7-tests)
  - [Tests by Module](#tests-by-module)
  - [`#[ignore]` Tests](#ignore-tests)
- [8. Branch and Commit Rules](#8-branch-and-commit-rules)
  - [When SQL Queries Are Modified (`.sqlx/`)](#when-sql-queries-are-modified-sqlx)
- [9. Test Tools (scripts/)](#9-test-tools-scripts)
  - [shire.sh (Docker Service Management)](#shiresh-docker-service-management)
  - [test.ollama.sh (Ollama Manual Test)](#testollamash-ollama-manual-test)
  - [dbbench (DB Storage Performance)](#dbbench-db-storage-performance)
  - [loadgen (Load Test)](#loadgen-load-test)
  - [Load Test Monitoring](#load-test-monitoring)

---
## 1. Prerequisites

- [Install Rust] https://www.rust-lang.org/tools/install

```bash
# Check the Rust compiler version
rustc --version

# Check the Cargo version
cargo --version
```

- [Install Docker] https://docs.docker.com/get-started/get-docker/

```bash
# Check the Docker version
docker --version

# Check the Docker Compose version
docker compose version
```

- Install CMake
  - Required when building the feature that lets the Rust server read events from Kafka
  - Related dependency: `rdkafka` (a library for using Kafka in Rust)

```bash
# Windows
winget install Kitware.CMake

# macOS
brew install cmake

# Linux (Ubuntu)
sudo apt install cmake

# Check the CMake version (open a new terminal after installation and run)
cmake --version
```

- NVIDIA GPU
  - Required only when using the AI decision feature (Ollama); without a GPU, the `ollama` container does not run

```bash
# Check the GPU
nvidia-smi
```

---

## 2. Project Initialization

```bash
git clone https://github.com/1Tsw0rd/shire-guard.git
cd shire-guard

# Create the environment variable files and enter the values (see 5. Environment Variables for item descriptions)
cp docker/.env.example docker/.env
cp backend/.env.example backend/.env
```

- If the `docker/.env` file does not exist, Docker fails to run
- Because `Cargo.toml` and `Cargo.lock` are included in the repository, dependencies are downloaded on `cargo build` without a separate `cargo add`

---

## 3. Docker Compose

### 3.1 Choosing the Message Broker and Cache

Shire Guard uses **one choice from each** of the two items below

| Category | Options | Default |
|---|---|---|
| Message broker | Kafka / RedPanda | Kafka |
| Cache | Redis / Dragonfly | Redis |

- RedPanda is an alternative program that works the same way as Kafka, and Dragonfly is an alternative program that works the same way as Redis
- If you use the default (Kafka + Redis), you do not need to change anything

#### How to Choose

Modify the values below in `docker/.env`

```dotenv
# kafka or redpanda
MESSAGE_BROKER=kafka

# redis or dragonfly
CACHE_BACKEND=redis
```

#### If You Chose RedPanda

`docker/vector/vector.toml` must also be modified

- This is because Vector (the event collector) sends events to the broker written in its configuration file regardless of the `MESSAGE_BROKER` value
- Comment out the `[sinks.kafka_out]`, `[sinks.kafka_out.buffer]`, and `[sinks.kafka_out.acknowledgements]` sections
- Uncomment the `[sinks.redpanda_out]`, `[sinks.redpanda_out.buffer]`, and `[sinks.redpanda_out.acknowledgements]` sections

### 3.2 Running

```bash
cd docker

# Validate the configuration (checks for missing required environment variables, etc.; no output if there is no problem)
docker compose config -q

# Run all services (15: includes both Kafka and RedPanda, and both Redis and Dragonfly)
docker compose up -d

# Example of running only the required services (Kafka + Redis combination)
# If you chose RedPanda or Dragonfly, replace kafka → redpanda and redis → dragonfly before running
docker compose up -d vector kafka redis postgres opensearch opensearch-init opensearch-dashboards clickhouse ollama prometheus grafana loki cadvisor

# Check the running status
docker compose ps
```

#### Downloading AI Models

Required when using Shire Guard > Playbook > AI response node

```bash
docker exec -it ollama ollama pull qwen2.5:14b-instruct
docker exec -it ollama ollama pull hf.co/fdtn-ai/Foundation-Sec-8B-Reasoning-Q8_0-GGUF

# Check the downloaded models
docker exec -it ollama ollama list

# NAME
# hf.co/fdtn-ai/Foundation-Sec-8B-Reasoning-Q8_0-GGUF:latest
# qwen2.5:14b-instruct
```

### 3.3 Operation

#### Checking Logs

```bash
# All logs (starting from the most recent 100 lines)
docker compose logs -f --tail=100

# Logs by service (multiple can be specified)
docker compose logs -f --tail=100 vector
docker compose logs -f --tail=100 kafka
docker compose logs -f --tail=100 grafana prometheus loki
```

#### Shutting Down

```bash
# Stop·remove containers (stored data is kept)
docker compose down
```

#### Data Initialization (If Needed)

- Data folder location: `docker/<service name>/data` (postgres, clickhouse, opensearch, kafka, redpanda, etc.)

```bash
# Run in the docker/ directory (run in Git Bash)
docker compose down <service name>
rm -rf <service name>/data
docker compose up -d <service name>
```

> For reference materials by service, see [note-docker-services.md](notes/note-docker-services.md)

### 3.4 Access Addresses

#### Web UI

| Service | Address | Notes |
|---|---|---|
| Grafana | http://localhost:3000 | Default account admin / admin |
| OpenSearch Dashboards | http://localhost:5601 | No login |
| Prometheus | http://localhost:9090 | Check the collection status in Status → Target health |
| ClickHouse Play | http://localhost:8123/play | `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` |
| cAdvisor | http://localhost:8082 | CPU·memory per container |

#### API / Status Check

| Service | Address | Notes |
|---|---|---|
| Vector event receiving | http://localhost:8081 | Destination for sending events (POST, JSON) |
| Vector health | http://localhost:8686/health | |
| Vector metrics | http://localhost:9598/metrics | |
| OpenSearch REST | http://localhost:9200 | |
| Ollama API | http://localhost:11434 | |

- Loki has no separate UI and is queried from Grafana
- PostgreSQL, Redis/Dragonfly, and Kafka/RedPanda have no web UI

### 3.5 Grafana Setup (Optional)

To use Grafana, the 2 steps below must be performed

#### Step 1: Create Data Sources

Because the dashboard JSON references data sources by UID, the data sources must be created with the UIDs below

| Data Source | UID | URL (from the Grafana container) |
|---|---|---|
| Prometheus | `afxri9wxzbzlsc` | `http://prometheus:9090` |
| Loki | `ffya6s2679edce` | `http://loki:3100` |

```bash
# The UID cannot be specified in the UI, so create them with the API (run in Git Bash)
curl -u admin:admin -X POST http://localhost:3000/api/datasources -H "Content-Type: application/json" -d '{"name":"prometheus","type":"prometheus","uid":"afxri9wxzbzlsc","url":"http://prometheus:9090","access":"proxy"}'

curl -u admin:admin -X POST http://localhost:3000/api/datasources -H "Content-Type: application/json" -d '{"name":"loki","type":"loki","uid":"ffya6s2679edce","url":"http://loki:3100","access":"proxy"}'
```

#### Step 2: Import Alerts·Dashboards

After creating the data sources, access Grafana (http://localhost:3000) and proceed

**Alerts**

Grafana left menu Alerting → Alert rules → More in the upper right → Import alert rules → configure as below, then Import
- Upload file path: `shire-guard/docker/grafana/imports/shire-guard-alerts.yaml`

<img src="assets/grafana-import-alert-rules.png" alt="Alert import settings" width="600">

**Dashboards**

Grafana left menu Dashboards -> shire-guard -> select shire-guard-alerts.yaml (see the screenshot below) -> New in the upper right -> Import dashboard -> Upload dashboard JSON file ->  select `shire-guard/docker/grafana/imports/shire-guard-dashboards.json`, then Import

<img src="assets/grafana-import-dashboard.png" alt="Dashboard import settings" width="600">

---

## 4. Running the Server

Proceed while the Docker services are running

```bash
cd backend

# Run with an optimized build (--release) (the first run takes a long time to build)
cargo run --release
```

- The log level is adjusted with `RUST_LOG` in `backend/.env`

```bash
# Server status (normal response: ok) / check Metrics
curl http://localhost:3001/health
curl http://localhost:3001/metrics
```

---

## 5. Environment Variables

| File | Responsibility |
|---|---|
| `docker/.env` | The environment variable file needed to operate the Docker services, used by both Docker Compose and the Backend |
| `backend/.env` | Rust Backend only |

### `docker/.env`

| Section | Variable | Default | Description |
|---|---|---|---|
| Ollama | `OLLAMA_PORT` | `11434` | Ollama port |
| Vector | `VECTOR_PORT` | `8081` | Event receiving port |
| | `VECTOR_API_PORT` | `8686` | API·health port |
| | `VECTOR_METRICS_PORT` | `9598` | Metric port |
| Message Broker | `MESSAGE_BROKER` | `kafka` | Broker used by the Backend: `kafka` or `redpanda` |
| | `KAFKA_HOST` | `localhost` | Host the Backend uses to connect to Kafka |
| | `KAFKA_PORT` | `9092` | Kafka port for communication between containers (Vector → Kafka) |
| | `KAFKA_EXTERNAL_PORT` | `19092` | Port the Backend uses to connect to Kafka |
| | `KAFKA_CONTROLLER_PORT` | `9093` | Kafka internal management (controller) port |
| | `KAFKA_TOPIC` | `test_topic` | Name of the topic in which events are stored |
| | `KAFKA_GROUP_ID` | `shire-guard-enrichment` | Backend Consumer Group ID |
| | `REDPANDA_HOST` | `localhost` | Host the Backend uses to connect to RedPanda |
| | `REDPANDA_PORT` | `29092` | Port the Backend uses to connect to RedPanda |
| OpenSearch | `OPENSEARCH_PORT` | `9200` | REST API port |
| | `OPENSEARCH_PERF_PORT` | `9600` | Performance analysis API port |
| | `OPENSEARCH_INITIAL_ADMIN_PASSWORD` | Set yourself | Initial administrator password (required value to run OpenSearch) |
| | `OPENSEARCH_DASHBOARDS_PORT` | `5601` | OpenSearch Dashboards port |
| ClickHouse | `CLICKHOUSE_HTTP_PORT` | `8123` | HTTP port |
| | `CLICKHOUSE_NATIVE_PORT` | `9000` | Native port (for CLI connection) |
| | `CLICKHOUSE_DB` | `shire` | Database name |
| | `CLICKHOUSE_USER` | `shire` | User |
| | `CLICKHOUSE_PASSWORD` | Set yourself | Password |
| PostgreSQL | `POSTGRES_PORT` | `5432` | Port |
| | `POSTGRES_DB` | `shire` | Database name |
| | `POSTGRES_USER` | `shire` | User |
| | `POSTGRES_PASSWORD` | Set yourself | Password |
| Key-Value Store | `CACHE_BACKEND` | `redis` | Cache used by the Backend: `redis` or `dragonfly` |
| | `REDIS_PORT` | `6379` | Redis port |
| | `REDIS_PASSWORD` | Set yourself | Redis password |
| | `DRAGONFLY_PORT` | `6380` | Dragonfly port |
| | `DRAGONFLY_PASSWORD` | Set yourself | Dragonfly password |
| Grafana | `GRAFANA_PORT` | `3000` | Grafana port |
| Prometheus | `PROMETHEUS_PORT` | `9090` | Prometheus port |
| cAdvisor | `CADVISOR_PORT` | `8082` | cAdvisor port |
| Loki | `LOKI_PORT` | `3100` | Loki port |

- The Rust Backend also reads the OpenSearch, ClickHouse, and Ollama connection information from `docker/.env` → do not define them again in `backend/.env`

### `backend/.env`

| Variable | Description |
|---|---|
| `DATABASE_URL` | PostgreSQL connection URL (URL encoding is needed if the password contains `@`, `:`, or `/`) |
| `RUST_LOG` | Log level by module |
| `ABUSEIPDB_API_KEY` | AbuseIPDB API key |
| `VIRUSTOTAL_API_KEY` | VirusTotal API key |
| `DNS_RESOLVER_HOST` / `DNS_RESOLVER_PORT` | DNS lookup server |

---

## 6. Commands Used to Add Rust Dependencies

### Application Dependencies

```bash
# Update dependencies to the latest versions within the version conditions of Cargo.toml
# To update only a specific package: cargo update -p axum
cargo update

# Web framework: /health, /metrics endpoints
cargo add axum@0.8.9

# Asynchronous runtime
# full: all features such as networking, timers, synchronization
# test-util: feature for virtually controlling time in tests
cargo add tokio@1.53.1 --features full,test-util

# Macro for defining async fn in traits
cargo add async-trait@0.1.92

# Supports serialization and deserialization, which convert data structures into other formats
# derive: allows the required conversion features to be implemented automatically with macros
cargo add serde@1.0.229 --features derive
cargo add serde_json@1.0.151

# Handles dates and times (serde: allows date and time data to be serialized and deserialized)
cargo add chrono@0.4.45 --features serde

# .env environment variable loader
cargo add dotenvy@0.15.7

# Outputs and filters the logs generated by tracing
# fmt: sets the output format of logs
# env-filter: allows the log level to be adjusted with the RUST_LOG environment variable
cargo add tracing@0.1.44
cargo add tracing-subscriber@0.3.23 --features fmt,env-filter

# Provides middleware that traces HTTP requests and responses
# trace: allows trace logs to be left during request processing
cargo add tower-http@0.7.1 --features trace

# Message broker client that communicates with Kafka and RedPanda
# cmake-build: builds librdkafka, the required native library, with CMake
cargo add rdkafka@0.39.0 --features cmake-build

# Redis / Dragonfly client
# tokio-comp: supports Tokio-based asynchronous communication / connection-manager: supports automatic reconnection of dropped connections
cargo add redis@1.7.0 --features tokio-comp,connection-manager

# HTTP client
# json: JSON body handling / query: supports setting URL query parameters / rustls: supports TLS encrypted communication
cargo add reqwest@0.13.5 --features json,query,rustls

# Performs DNS lookups (disables the default features and uses only the Tokio-based asynchronous lookup feature)
cargo add hickory-resolver@0.26.3 --no-default-features --features tokio

# IP/CIDR handling
cargo add ipnet@2.12.2

# SHA-256 hash generation
cargo add sha2@0.11.0

# PostgreSQL asynchronous SQL toolkit
# runtime-tokio: uses the Tokio runtime
# tls-rustls: supports Rustls-based TLS connections
# postgres: enables the PostgreSQL driver
# uuid, chrono, json: integrates those types with the database
# macros: uses macros such as query! and compile-time query validation
cargo add sqlx@0.9.0 --features runtime-tokio,tls-rustls,postgres,uuid,chrono,macros,json

# UUID v4 generation
# v4: supports random-based UUID v4 generation
# serde: supports UUID serialization and deserialization
cargo add uuid@1.26.0 --features v4,serde

# Collects and exposes Prometheus Metrics
cargo add prometheus@0.14.0

# Request DTO validation
cargo add validator@0.21.0 --features derive
```

### Test-only Dependencies

```bash
# Enables the utility that simulates HTTP requests in memory (.oneshot) without actually running the server
cargo add tower@0.5.3 --dev --features util

# HTTP Body collection·verification utility: a tool that collects the stream byte body (Body) data returned by Axum and converts it into human-readable JSON/text
cargo add http-body-util@0.1.5 --dev

# Creating and verifying test JSON requests·responses
cargo add serde_json@1.0.151 --dev

```

---

## 7. Tests

```bash
# All unit tests (same as CI, excluding ignored)
cargo test --lib

# Check only the test list
cargo test --lib -- --list

# Also check the output logs
cargo test --lib -- --nocapture

# Everything including ignored (all execution conditions of the `#[ignore]` tests below are required)
cargo test --lib -- --include-ignored
```

### Tests by Module

```bash
# Environment variable parsing
cargo test --lib config::tests

# Consumer
cargo test --lib core::consumer::event
cargo test --lib core::consumer::offset

# Enrichment
cargo test --lib core::enrichment::circuit_breaker
cargo test --lib core::enrichment::evidence
cargo test --lib core::enrichment::provider::tests
cargo test --lib core::enrichment::service
cargo test --lib core::enrichment::providers::dns

# Playbook
cargo test --lib core::playbook::condition
cargo test --lib core::playbook::nodes
cargo test --lib core::playbook::engine
cargo test --lib core::playbook::ai

# Storage
cargo test --lib core::storage::forwarder
cargo test --lib core::storage::record
cargo test --lib core::storage::retry

# OpenSearch
cargo test --lib common::clients::opensearch

# Run only one test function (the path includes tests::)
cargo test --lib core::consumer::offset::tests::holds_position_until_earlier_offset_finishes
cargo test --lib core::playbook::engine::tests::majority_vote_two_thirds_wins
```

### `#[ignore]` Tests

These perform actual API requests or DB writes, so run them only in the local development environment

```bash
# Requires the ClickHouse container to be running
cargo test --lib common::clients::clickhouse -- --ignored

# Requires the PostgreSQL container + DATABASE_URL in backend/.env
cargo test --lib core::storage::failures -- --ignored

# Requires the API keys in backend/.env; uses the actual API quota
cargo test --lib core::enrichment::providers::abuseipdb -- --ignored
cargo test --lib core::enrichment::providers::virustotal -- --ignored

# Requires access to an external DNS server (1.1.1.1)
cargo test --lib core::enrichment::providers::dns -- --ignored

# Everything including ignored
cargo test --lib -- --include-ignored
```

---

## 8. Branch and Commit Rules

`develop` is used as the integration branch; changes are made in a branch for each task and merged into `develop` through a Pull Request

```text
develop
└── feature / docs / bugfix, etc. branches
```

Commit messages use the following format

```text
type: description of the work
```

- The examples below are actual commit messages of this repository, so they are kept in Korean (English meaning in parentheses)

| Type | Purpose | Example |
|---|---|---|
| `feat` | Add a feature | `feat: 로깅 완료 후 Kafka commit 기능 추가` (Add Kafka commit after logging completes) |
| `fix` | Fix a bug | `fix: postgreSQL 초기화 sql 내 불필요한 쿼리 제거` (Remove unnecessary queries in the PostgreSQL initialization SQL) |
| `docs` | Write·modify documents | `docs: README 아키텍처·주요 기능·성능 문서 작성` (Write README architecture·main features·performance documents) |
| `chore` | Project management·comments, etc. | `chore: 오탈자 수정 및 주석 제거` (Fix typos and remove comments) |
| `build` | Dependency·build settings | `build: release 빌드 LTO 설정 추가` (Add LTO setting to the release build) |
| `test` | Add·modify tests | `test: offset 추적기 재전송 시나리오 추가` (Add redelivery scenario for the offset tracker) |
| `refactor` | Structural improvement without functional changes | `refactor: ingested_at 필수 필드로 승격` (Promote ingested_at to a required field) |
| `style` | Format·whitespace, etc. | `style: cargo fmt 적용` (Apply cargo fmt) |
| `perf` | Performance improvement | `perf: 로그 저장 성능 튜닝` (Tune log storage performance) |

When Rust work is finished, run the following in `backend/` and then commit

```bash
cargo fmt
cargo clippy --all-targets --all-features -- -D warnings
cargo test --lib
```

- GitHub Actions CI runs `fmt --check` → `clippy` → `build` → `test --lib` on push / Pull Request to the `develop` and `main` branches
- If you modified an SQL query (`sqlx::query!`), you also need to run `cargo sqlx prepare` below and commit `.sqlx/` together

### When SQL Queries Are Modified (`.sqlx/`)

- The `sqlx::query!` macro connects to the DB **at compile time** and checks the SQL syntax and column types
- GitHub CI has no PostgreSQL, so it cannot check SQL directly; instead, it uses the check results saved locally in advance (`backend/.sqlx/`)
- Therefore, when you add·modify a query, you must regenerate the check results locally and commit them together (otherwise the CI build fails)

```bash
# Install sqlx-cli (only once at first, matching the project's sqlx version·features)
cargo install sqlx-cli --version 0.9.0 --no-default-features --features rustls,postgres

# Run in backend/ while PostgreSQL is running
# --all-targets: also includes queries in test code
cargo sqlx prepare -- --all-targets
```

---

## 9. Test Tools (scripts/)

| Tool | Description |
|---|---|
| [`shire.sh`](../scripts/shire.sh) | A collection of management commands for running · logs · access, etc. of Docker services |
| [`test.ollama.sh`](../scripts/test.ollama.sh) | A manual test tool that calls an Ollama model directly to check its response |
| [`dbbench`](../scripts/dbbench) | A tool that measures OpenSearch · ClickHouse storage performance |
| [`loadgen`](../scripts/loadgen) | A load test tool that sends a large number of events to Vector |

### shire.sh (Docker Service Management)

A collection of management commands for logs · access · start/stop, etc. by service (see `--help` for the command list)

```bash
# Commands by service (logs · exec, etc.) require the corresponding Docker service to be running (except start)
cd scripts

# Check the usage (same as running without the --help argument)
./shire.sh

# Example: check Kafka logs, connect to ClickHouse
./shire.sh kafka logs
./shire.sh clickhouse exec
```

### test.ollama.sh (Ollama Manual Test)

Calls an Ollama model directly to check the response (see `--help` for usage, [example of execution results](samples/sample-test-ollama.md))

```bash
# The ollama container must be running; to change the test prompt, modify SYSTEM_PROMPT / PROMPT at the top of the script
cd scripts

# Check the usage (same as running without the --help argument)
./test.ollama.sh

# Example: call model 1 (Qwen), translate the response into Korean
./test.ollama.sh ollama 1 --ko
```

### dbbench (DB Storage Performance)

Measures speed by storing a large amount of data directly in OpenSearch / ClickHouse without the pipeline (see `--help` for options and behavior)

```bash
cd scripts

# Build (when the code changes): copy the output dbbench/target/release/dbbench.exe to scripts
(cd dbbench && cargo build --release) && cp dbbench/target/release/dbbench.exe .

# Check the usage (same as running without the --help argument)
./dbbench.exe --help

# Example: store a total of 500,000 documents in OpenSearch / ClickHouse
./dbbench.exe os 500000 --batch 1000 --concurrency 4
./dbbench.exe ch 500000 --batch 10000 --concurrency 4
```

### loadgen (Load Test)

A test tool that sends a large number of test events to Vector to verify the entire Shire Guard pipeline (collection → Enrichment → playbook decision → storage → failure retention) (see `--help` for options and behavior)

```bash
# Vector · Backend · storage must be running (with the -v option, the service status is checked and printed before running)
cd scripts

# Build (when the code changes): copy the output loadgen/target/release/loadgen.exe to scripts
(cd loadgen && cargo build --release) && cp loadgen/target/release/loadgen.exe .

# Check the usage (same as running without the --help argument)
./loadgen.exe --help

# Interactive setup: answer the questions to set the event type / count / concurrency, etc. and run
./loadgen.exe setup

# Example: file_download / dangerous / total 15000 events / concurrency 1000
./loadgen.exe file_download 1 15000 1000

# Example: login_failure / safe / total 10000 events + 10 failures + 10 rejections
./loadgen.exe login_failure 15001 10000 --case safe --fail 10 --reject 10

# How to check the results (web addresses, check commands)
./loadgen.exe check
```

- The default output is 1 result line per round; with the `-v` option, the expected values of this run are also printed after the run (compare with the check results of `./loadgen.exe check`)
- If you send events while the Backend is turned off, the events accumulate in Kafka, and you can turn on the Backend to measure the processing speed of the accumulated events


### Load Test Monitoring

Run in a separate terminal while sending with loadgen (exit with Ctrl+C)

```bash
# Consumed / stored counts and per-second speed (1-second interval, the Backend must be running)
# Output labels: 소비=consumed, 소비속도=consume rate, 저장완료=stored, 저장속도=store rate, 재시도=retries
metric() { curl -s 127.0.0.1:3001/metrics | awk -v n="$1" '$0 ~ "^"n {s+=$2} END{print int(s)}'; }
prev_r=$(metric shireguard_consumer_messages_received_total); prev_s=$(metric shireguard_storage_events_sent_total); prev_t=$(date +%s)
while true; do
  sleep 1
  recv=$(metric shireguard_consumer_messages_received_total)
  sent=$(metric shireguard_storage_events_sent_total)
  retry=$(metric shireguard_storage_retries_total)
  now=$(date +%s); dt=$((now-prev_t)); [ $dt -le 0 ] && dt=1
  echo "$(date +%T)  소비=${recv}  소비속도=$(( (recv-prev_r)/dt ))/s  저장완료=${sent}  저장속도=$(( (sent-prev_s)/dt ))/s  재시도=${retry}"
  prev_r=$recv; prev_s=$sent; prev_t=$now
done

# Container CPU / memory (2-second interval; when using RedPanda, kafka → redpanda)
while true; do
  docker stats --no-stream --format "{{.Name}}  CPU {{.CPUPerc}}  MEM {{.MemUsage}}" opensearch clickhouse kafka
  echo "--- $(date +%T)"; sleep 2
done

# Number of accumulated messages in the Kafka topic (5-second interval, when using Kafka)
# Output label: 토픽누적=accumulated messages in the topic
while true; do
  echo "$(date +%T)  토픽누적=$(MSYS_NO_PATHCONV=1 docker exec -e KAFKA_OPTS= kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic test_topic --time -1 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')"
  sleep 5
done

# After measurement: average response time by storage
# Output format: <storage>  요청 N회 (N requests)  평균 N초 (average N seconds)
curl -s 127.0.0.1:3001/metrics | awk '
/^shireguard_storage_request_seconds_(sum|count)/ {
  match($0, /db="[a-z]+"/); d = substr($0, RSTART+4, RLENGTH-5)
  if ($0 ~ /_sum/) s[d] = $2; else c[d] = $2
}
END { for (d in s) printf "%-10s 요청 %d회  평균 %.2f초\n", d, c[d], s[d]/c[d] }'
```

```powershell
# Rust Backend CPU (2-second interval, PowerShell)
# It is the sum of per-core usage, so it can exceed 100% (e.g., 1600 if all 16 cores are used)
while ($true) { (Get-Counter '\Process(backend)\% Processor Time').CounterSamples.CookedValue; Start-Sleep 2 }
```
