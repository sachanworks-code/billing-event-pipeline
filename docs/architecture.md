# Architecture and tradeoffs

## Event contract

Each event has a caller-generated UUID `eventId`, `invoiceId`, `customerId`, positive decimal `amount`, three uppercase-letter `currency`, and ISO `billingDate`. Amounts have at most 12 integer digits and two fractional digits. IDs are nonblank and at most 64 characters. Unknown JSON properties are rejected. Currency validation checks the format, not membership in an ISO currency list; customer, tax, and invoice-domain validation are deliberately outside the example.

The REST endpoint validates before publishing. The listener validates again because producers can bypass HTTP. Both use the same record contract. Amounts are normalized to scale 2 so `12.5` and `12.50` are equivalent. Event schema changes require a topic/version migration and a deliberate compatibility policy.

## Transactions and idempotency

1. Insert the billing record using `eventId` as the primary key.
2. If that ID already exists, take a shared locking read and compare the canonical payload. Identical replays finish successfully; conflicting replays raise a permanent error.
3. For a new event, insert its audit payload into the outbox.
4. Commit both rows before the Kafka listener returns.

The insert relies on the unique constraint, not a check-then-insert race. MySQL's duplicate-key statement failure does not abort the whole JDBC transaction. A shared locking read avoids lock upgrades between concurrent replays and also avoids a stale repeatable-read snapshot when another concurrent transaction wins the insert.

If the process crashes after DB commit but before the input offset is committed, Kafka redelivers the event. The primary key makes that replay safe.

## Audit relay

The relay runs once per second and sends at most 20 events per cycle. Each event is handled in a separate database transaction:

1. Select the oldest pending outbox row with `FOR UPDATE SKIP LOCKED`.
2. Publish to Kafka, keyed by the input event ID, and wait for broker acknowledgement.
3. Set `published_at` and commit.

Another relay skips rows already claimed by a publisher. The transaction deliberately holds one row lock and one DB connection during the broker call; this is simple for a demo but limits throughput during Kafka latency. A production design could use CDC or a claim/lease relay with bounded concurrency.

If publishing fails, the transaction rolls back and the next scheduled cycle retries. If Kafka accepts the record but the database update or commit fails, publication can occur again. Audit payloads contain a stable `auditId` (the original event ID), so consumers can deduplicate. Kafka producer idempotence reduces retries within a producer session; it does not make the DB/Kafka boundary exactly once.

There is no global audit ordering guarantee. Partitioning is by event ID, and concurrent relays can publish out of creation order. Published rows are retained in this demo; real systems need cleanup and archival policies.

## Listener retries and DLQ

Transient failures get two retries with a one-second fixed backoff: three total attempts. Permanent input errors skip retries. Recovery publishes the original string value to `billing.events.v1.dlq` on the matching partition, with exception/source headers. The handler requires acknowledgement of DLQ publication; if that publish fails, it throws and the input record remains eligible for redelivery.

Retries block progress on the affected input partition; the topic has three partitions. During a sustained database outage, retry exhaustion moves events to the DLQ rather than preserving them indefinitely in the input stream. Production policy may instead pause consumers during infrastructure outages. There is no automated DLQ replay or operator UI here.

## Why JDBC and an outbox?

Explicit SQL makes the unique-key, rollback, and row-lock behavior visible to reviewers. The outbox closes the audit-loss window that would exist if the listener committed MySQL and then independently published to Kafka. Kafka transactions alone cannot atomically commit a MySQL transaction.

## References

- [Spring Kafka exception handling](https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html)
- [MySQL locking reads](https://dev.mysql.com/doc/refman/8.4/en/innodb-locking-reads.html)
- [Testcontainers Kafka module](https://java.testcontainers.org/modules/kafka/)
