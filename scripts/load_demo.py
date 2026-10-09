#!/usr/bin/env python3
"""Measure local HTTP acceptance and completion using synthetic events. No third-party dependencies."""
import argparse
import concurrent.futures
import datetime
import json
import math
import os
import time
import urllib.error
import urllib.request
import uuid


def request(url, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json", "X-API-Key": os.environ.get("BILLING_API_KEY", "billing-local-dev-key")})
    with urllib.request.urlopen(req, timeout=25) as response:
        return response.status, json.load(response)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8080")
    parser.add_argument("--events", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args()
    if min(args.events, args.concurrency, args.timeout) < 1:
        parser.error("events, concurrency, and timeout must be positive")
    endpoint = args.base_url.rstrip("/") + "/api/v1/billing-events"
    events = [{"eventId": str(uuid.uuid4()), "invoiceId": "LOAD-DEMO-" + str(i),
               "customerId": "SYNTHETIC-CUSTOMER", "amount": 1499.00, "currency": "INR",
               "billingDate": datetime.date.today().isoformat()} for i in range(args.events)]
    started = time.perf_counter()

    def submit(event):
        before = time.perf_counter()
        try:
            status, _ = request(endpoint, event)
            return event["eventId"], (time.perf_counter() - before) * 1000, status == 202, None
        except Exception as exc:
            return event["eventId"], (time.perf_counter() - before) * 1000, False, str(exc)

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        submitted = list(pool.map(submit, events))
        acceptance_seconds = time.perf_counter() - started
        pending = {event_id for event_id, _, accepted, _ in submitted if accepted}
        completed = set()
        deadline = time.monotonic() + args.timeout

        def is_complete(event_id):
            try:
                _, body = request(endpoint + "/" + event_id)
                return event_id if body.get("auditStatus") == "PUBLISHED" else None
            except urllib.error.HTTPError as exc:
                if exc.code == 404:
                    return None
                raise

        lookup_errors = []
        while pending and time.monotonic() < deadline:
            for future in [pool.submit(is_complete, event_id) for event_id in pending]:
                try:
                    event_id = future.result()
                    if event_id:
                        completed.add(event_id)
                except Exception as exc:
                    lookup_errors.append(str(exc))
            pending -= completed
            if pending:
                time.sleep(0.25)
    latencies = sorted(ms for _, ms, accepted, _ in submitted if accepted)
    percentile = lambda p: round(latencies[max(0, math.ceil(len(latencies) * p) - 1)], 2) if latencies else None
    elapsed = time.perf_counter() - started
    report = {"events": args.events, "concurrency": args.concurrency,
              "accepted": len(latencies), "fully_processed": len(completed),
              "acceptance_seconds": round(acceptance_seconds, 3),
              "acceptance_requests_per_second": round(len(latencies) / acceptance_seconds, 2),
              "acceptance_latency_p50_ms": percentile(0.50), "acceptance_latency_p95_ms": percentile(0.95),
              "total_seconds_including_completion_polling": round(elapsed, 3),
              "submission_errors": [error for _, _, ok, error in submitted if not ok],
              "lookup_errors": lookup_errors[:5], "pending_after_timeout": len(pending)}
    print(json.dumps(report, indent=2))
    raise SystemExit(0 if len(completed) == args.events else 1)


if __name__ == "__main__":
    main()
