# Operational notes

## Configuration

| Setting | Default | Purpose |
| --- | --- | --- |
| `DB_URL` | `jdbc:mysql://localhost:3306/billing` | Database connection |
| `DB_USER` | `billing` | Local demo database user |
| `DB_PASSWORD` | `billing_local_only` | Local demo password |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Bootstrap broker |
| `billing.retry.max-retries` | `2` | Retries after the first delivery |
| `billing.retry.delay-ms` | `1000` | Blocking listener backoff |
| `billing.outbox.poll-ms` | `1000` | Delay between relay cycles |
| `billing.outbox.enabled` | `true` | Allows disabling the scheduled relay |

Spring settings can also be overridden through their standard environment-variable forms, e.g. `BILLING_OUTBOX_ENABLED=false`. Flyway owns database schema migrations.

## Health and metrics

`GET /actuator/health` reports application/datasource health. It is not an end-to-end billing probe and does not certify that Kafka audit publication is progressing. Actuator exposes standard JVM, HTTP, datasource, and available Kafka metrics at `/actuator/metrics`. There is no Prometheus scrape endpoint in this version.

Useful DB checks (run using the demo credentials inside the MySQL container):

```sql
SELECT COUNT(*) AS pending_audits,
       MIN(created_at) AS oldest_pending
FROM audit_outbox WHERE published_at IS NULL;

SELECT COUNT(*) AS persisted_events FROM billing_event;
```

A growing pending count is a signal to inspect broker connectivity and app logs. Monitor input consumer lag and DLQ traffic as well. Only event errors are dead-lettered; outbox publication errors stay pending and are logged for retry.

## Replay and failure interpretation

- A `404` lookup can mean not yet processed, not submitted, or rejected asynchronously. This version has no acceptance/rejection status ledger.
- A `PUBLISHED` audit status records a broker acknowledgement, not downstream consumption.
- A failed HTTP acknowledgement is ambiguous; resend the same event ID and payload.
- DLQ headers preserve failure context. Fix invalid events before replay; a payload conflict cannot overwrite an already persisted event.
- Kafka data is ephemeral in the Compose demo. Do not treat it as a durable hosted installation.

## Troubleshooting

**Ports in use:** stop conflicting local services or change the host port mappings. When running from source, update datasource and bootstrap settings to match any changed ports.

**Docker unavailable during tests:** start Docker Desktop. On macOS, if the Docker context uses a non-default socket, set `DOCKER_HOST=unix://$HOME/.docker/run/docker.sock` for the Maven invocation.

**Old database schema:** keep existing Flyway migrations immutable and add a new numbered migration. Use a separate demo volume for experiments that need a fresh DB.

**Kafka startup:** inspect `docker compose logs kafka` and wait for the health check before starting the app. No ZooKeeper is needed; the single broker uses KRaft.

## Hosting checklist

Add API authentication/authorization and request limits; replace local credentials with secrets; configure Kafka authentication and TLS; use multiple brokers with appropriate replication; restrict management endpoints; define retention, outbox cleanup, replay tooling, and operational alerts. Benchmark with realistic payloads and failure modes before making capacity claims.
