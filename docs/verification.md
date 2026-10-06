# Local verification

Verified on 6 October 2026 using Java 21.0.4 on an arm64 macOS host and Docker Desktop with Docker Engine 29.8.1. Docker reported approximately 7.9 GB available memory. Tests used real Kafka 3.9.1 and MySQL 8.4 containers.

## Checks completed

- Maven `verify`: **26 tests passed**, zero failures, errors, or skips.
  - 12 event-contract tests.
  - 5 HTTP API tests.
  - 9 Kafka/MySQL integration tests, including concurrent duplicates, retry exhaustion, transaction rollback, and audit replay after acknowledgement.
- Maven wrapper successfully downloaded and launched Maven 3.9.16 under Java 21.
- Compose configuration validated; Docker image built from source and ran as a non-root user.
- Full Compose stack started; MySQL and Kafka health checks passed; application health returned `UP`.
- Synthetic load demo with 100 events and 8 concurrent submissions: **100 accepted, 100 fully processed**, zero submission or lookup errors.

## Load-demo interpretation

The observed HTTP acceptance p50 was 10.19 ms and p95 was 212.55 ms. All events were persisted and had audit publication acknowledged within 4.992 seconds of starting the demo, including completion polling. Raw results are in [load-results.json](load-results.json).

This was a small local smoke run immediately after startup, not a sustained-load benchmark. It includes client/network overhead and polling delay. It does not establish production throughput, per-event end-to-end latency, capacity, availability, or performance under failure. Rerun `scripts/load_demo.py` on your target environment and document warmup, duration, hardware, and payload distribution before making capacity claims.

The database/broker failure tests inject failures at the processor, database, and audit-sender boundaries. They verify recovery semantics; they do not simulate a complete network partition or host crash.
