# Dashboard operations

## Start and connect

`docker compose up --build -d` starts the complete dashboard on http://localhost:8080/. Local access key: `billing-local-dev-key`. Set `BILLING_API_KEY` in the environment or an ignored `.env` file to override it. Never commit real keys.

The static UI is served by Spring Boot. No Node runtime or separate frontend hosting is needed. All dashboard queries and event submissions use the existing API-key authentication. The UI stores no key in localStorage, cookies, or URLs.

## Receipt semantics

Each valid API request creates a separate `submissionId`, then sends the normalized event to Kafka with this ID in a header. `202` means broker acknowledged; it does not mean processing completed.

- SUBMITTING: receipt exists; broker acknowledgement not yet recorded.
- ACCEPTED: Kafka acknowledged the submission.
- PROCESSED: new billing record and outbox intent committed with the receipt transition.
- DUPLICATE: identical event already exists; no new billing record is inserted.
- FAILED: the DLQ observer persisted the failure and its receipt transition.
- UNKNOWN: acknowledgement was ambiguous. Processing may still finish. Retry the same event ID and identical payload.

The receipt insert and Kafka publish are separate operations. A process crash between them can leave SUBMITTING; this ledger does not promise automatic resubmission. A late HTTP acknowledgement cannot overwrite an already completed result. Audit publication time on a duplicate belongs to the original stored event; conflicting failed receipts do not inherit its audit state.

Summary queries are independent reads, so totals may briefly differ while events are processing. Activity counts API receipts only. Direct Kafka ingestion can produce bills or failures without a receipt. A failure counter counts DLQ records; duplicates in the DLQ with different offsets count separately. Failure history retries database persistence indefinitely in place and never republishes failures into the DLQ. DLQ observations of the same partition/offset are idempotent.

## Dashboard API

All endpoints below require `X-API-Key`.

| Endpoint | Result |
| --- | --- |
| GET /api/v1/dashboard/summary | Metrics, five-minute activity buckets, UTC server time |
| GET /api/v1/dashboard/submissions | Receipt page; optional search, status, page |
| GET /api/v1/dashboard/submissions/{submissionId} | Receipt, original event, timeline, latest failure reason |
| GET /api/v1/dashboard/failures | Dead-letter observations; optional page |
| GET /api/v1/dashboard/failures/{failureId} | Failure details and raw payload |

Pages contain up to 20 rows. Search is literal text (up to 64 characters) over invoice, customer, and event ID. Page indexes begin at zero. Unknown status filters and invalid pages return 400; missing IDs return 404. Responses use UTC timestamps. Raw payloads are escaped before display in the UI.

## Public hosting

A public URL needs a chosen hosting account and budget. The supplied Compose stack is a local demo, with fixed local database credentials, ephemeral Kafka, and loopback binding. Do not expose it unchanged.

Use a container-capable VM or service able to run Java, Kafka, and MySQL. Before publication configure HTTPS through a reverse proxy, replace database/API keys with host-managed secrets, keep Kafka/MySQL internal, persist Kafka and MySQL volumes, configure backups and retention, and restrict/rate-limit demo writes. A shared API key grants both reads and writes; a public visitor mode needs a separately scoped read-only design. This project currently has no anonymous data access.

## Verification

`./mvnw verify` exercises 32 tests, including 15 integration tests using real Kafka/MySQL. Dashboard coverage verifies processed/duplicate/conflicting receipts, audit state, missing authentication, public static shell, filter validation, and malformed DLQ payload visibility. Browser checks also exercise sample submission, receipt details, duplicate replay, and conflict diagnostics. These are functional checks, not a production capacity certification.
