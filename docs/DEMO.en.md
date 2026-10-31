# Shire-Guard Demo Scenarios

[한국어](DEMO.md) | **English**

- A document that summarizes Shire-Guard event processing, including demonstration videos

## Table of Contents
- [Arbitrarily Created Event Types Used in the Demo](#arbitrarily-created-event-types-used-in-the-demo)
- [Playbook Policies Used in the Demo](#playbook-policies-used-in-the-demo)
- [Tests by Scenario](#tests-by-scenario)
  - [1. Backlog Processing Performance](#1-backlog-processing-performance-batch-processing-of-1-million-events-accumulated-while-the-rust-server-was-stopped)
  - [2. Failure Recovery](#2-failure-recovery-no-loss-even-after-stopping-and-recovering-kafka--the-rust-server)
  - [3. Real-time Processing](#3-real-time-processing-processing-immediately-upon-receipt)

## Arbitrarily Created Event Types Used in the Demo
1) file_download (dangerous case)
- source_ip: AbuseIPDB score 100 (API response result as of 2026-10)
- file_sha256: 66 VirusTotal engines detected it as malicious (API response result as of 2026-10)
```json
{
  "event_id": "loadgen-1",
  "event_type": "file_download",
  "timestamp": "2026-10-28T10:00:00Z",
  "source_ip": "144.48.243.18",
  "destination_domain": "cdn-update-service.net",
  "file_name": "invoice_2026.exe",
  "file_sha256": "275a021bbfb6489e54d471899f7db9d1663fc695ec2fe2a2c4538aabf651fd0f",
  "file_size_bytes": 245760
}
```

2) login_failure (safe case)
- source_ip: AbuseIPDB score 0 (API response result as of 2026-10)
```json
{
  "event_id": "loadgen-1000001",
  "event_type": "login_failure",
  "timestamp": "2026-10-28T10:00:00Z",
  "source_ip": "118.25.6.39",
  "username": "admin",
  "attempt_count": 12,
  "target_service": "ssh"
}
```

3) dns_beacon (safe case)
- source_ip: AbuseIPDB score 0 (API response result as of 2026-10)
- destination_domain: a domain that the Playbook under test will not block
```json
{
  "event_id": "loadgen-2000001",
  "event_type": "dns_beacon",
  "timestamp": "2026-10-28T10:00:00Z",
  "source_ip": "118.25.6.39",
  "destination_domain": "nexon.com",
  "hostname": "WKS-1183",
  "request_interval_seconds": 60
}
```

4) Rust parsing failure event
- An event that has no event_id, so it fails to parse on the Rust server and is stored in PostgreSQL, which stores processing failure logs
```json
{
  "loadgen_id": "loadgen-failure-<number>",
  "event_type": "login_failure",
  "timestamp": "2026-10-28T10:00:00Z",
  "source_ip": "144.48.243.18",
  "username": "admin",
  "attempt_count": 12,
  "target_service": "ssh"
}
```

5) OpenSearch rejection event
- An event whose object field is a string, so OpenSearch rejects it and it is stored in PostgreSQL, which stores processing failure logs
```json
{
  "event_id": "loadgen-reject-<number>",
  "event_type": "login_failure",
  "timestamp": "2026-10-28T10:00:00Z",
  "source_ip": "144.48.243.18",
  "username": "admin",
  "attempt_count": 12,
  "target_service": "ssh",
  "enrichment_dst_domain": "x"
}
```

## Playbook Policies Used in the Demo
![#2 High-risk IP blocking](assets/playbook-2.png)

![#4 High-risk executable file blocking](assets/playbook-4.png)

## Tests by Scenario
![Demo scenarios](assets/demo-scenarios.png)

> Before every test, the docker services were stopped, the data was deleted, and the services were started again.

<!-- 
cd docker
docker compose down

# grafana/data is not deleted (keeps data sources and dashboards)
rm -rf kafka/data opensearch/data clickhouse/data postgres/data vector/data loki/data

docker compose up -d --scale redpanda=0 --scale dragonfly=0
-->

### 1. Backlog Processing Performance: Batch Processing of 1 Million Events Accumulated While the Rust Server Was Stopped

![Scenario 1 overview](assets/scenario-1-overview.png)

- A scenario in which 1 million events are accumulated in Kafka while the Rust server is turned off, and then the Rust server is started to process them all at once.

#### Expected Results
- 1 million Kafka events are processed without loss,
- 998,900 identical event logs are stored in both OpenSearch and ClickHouse
- 1,100 event logs that failed processing are stored in PostgreSQL

![Scenario 1 expected results](assets/scenario-1-result.png)

## 🎬 Demonstration Video

> ▶ Click an image to watch the video (mp4)

<!--
./loadgen.exe file_download 1 400000 500 --fail 1000 --reject 100 &
./loadgen.exe login_failure 1000001 300000 400 --case safe &
./loadgen.exe dns_beacon 2000001 298900 400 --case safe &
wait
-->

| Processing | Result Verification |
|:---:|:---:|
| <a href="assets/scenario-1-demo.mp4"><img src="assets/scenario-1-demo-preview.gif" width="320" alt="Scenario 1 processing"></a> | <a href="assets/scenario-1-verification.mp4"><img src="assets/scenario-1-verification-preview.gif" width="320" alt="Scenario 1 result verification"></a> |

---

### 2. Failure Recovery: No Loss Even After Stopping and Recovering Kafka · the Rust Server

![Scenario 2 overview](assets/scenario-2-overview.png)

- 5 million events are accumulated in Kafka while the Rust server is turned off, and then the Rust server is started to process them all at once.

- A scenario in which, during processing, the Kafka service is stopped and recovered -> the Rust server is stopped and recovered, and the 5 million events are processed without loss.

#### Expected Results
- 5 million Kafka events are processed without loss,
- 4,994,500 identical event logs are stored in both OpenSearch and ClickHouse
- 5,500 event logs that failed processing are stored in PostgreSQL

![Scenario 2 expected results](assets/scenario-2-result.png)

## 🎬 Demonstration Video

> ▶ Click an image to watch the video (mp4)

<!--
./loadgen.exe file_download 1 2000000 500 --fail 5000 --reject 500 &
./loadgen.exe login_failure 10000001 1500000 400 --case safe &
./loadgen.exe dns_beacon 20000001 1494500 400 --case safe &
wait
-->

| Processing | Result Verification |
|:---:|:---:|
| <a href="assets/scenario-2-demo.mp4"><img src="assets/scenario-2-demo-preview.gif" width="320" alt="Scenario 2 processing"></a> | <a href="assets/scenario-2-verification.mp4"><img src="assets/scenario-2-verification-preview.gif" width="320" alt="Scenario 2 result verification"></a> |

---

### 3. Real-time Processing: Processing Immediately Upon Receipt

![Scenario 3 overview](assets/scenario-3-overview.png)

- A scenario in which 1 million events are sent while the Rust server is running, and they are processed in real time.

#### Expected Results
- 1 million Kafka events are processed without loss,
- 998,900 identical event logs are stored in both OpenSearch and ClickHouse
- 1,100 event logs that failed processing are stored in PostgreSQL

![Scenario 3 expected results](assets/scenario-3-result.png)

## 🎬 Demonstration Video

> ▶ Click an image to watch the video (mp4)

<!--
./loadgen.exe setup
1        # [1] Add event
1        # file_download
2        # Case: dangerous
400000   # Count
1        # [1] Add event
2        # login_failure
1        # Case: safe
300000   # Count
1        # [1] Add event
3        # dns_beacon
1        # Case: safe
298900   # Count
1        # [1] Add event
4        # Failure event (no case question)
1000     # Count
1        # [1] Add event
5        # Rejection event (no case question)
100      # Count
3        # [3] Finish setup
1300     # Concurrency (same value as the command version 500 + 400 + 400)
Enter    # Start number: default 1
Enter    # Repeat count: default 1
1        # [1] Run
-->

| Processing | Result Verification |
|:---:|:---:|
| <a href="assets/scenario-3-demo.mp4"><img src="assets/scenario-3-demo-preview.gif" width="320" alt="Scenario 3 processing"></a> | <a href="assets/scenario-3-verification.mp4"><img src="assets/scenario-3-verification-preview.gif" width="320" alt="Scenario 3 result verification"></a> |
