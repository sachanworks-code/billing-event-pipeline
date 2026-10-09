package dev.sachanworks.billing;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.util.UUID;

@Service
public class SubmissionTracker {
    public static final String HEADER = "billing-submission-id";
    private final JdbcTemplate jdbc;
    private final EventCodec codec;
    private final BillingProcessor processor;
    public SubmissionTracker(JdbcTemplate jdbc, EventCodec codec, BillingProcessor processor) {
        this.jdbc = jdbc; this.codec = codec; this.processor = processor;
    }
    public UUID begin(BillingEvent event) {
        UUID id = UUID.randomUUID();
        jdbc.update("""
            INSERT INTO event_submission(submission_id, event_id, invoice_id, customer_id, amount, currency, payload, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'SUBMITTING')
            """, id.toString(), event.eventId().toString(), event.invoiceId(), event.customerId(), event.amount(), event.currency(), codec.encode(event.normalized()));
        return id;
    }
    public void accepted(UUID id) {
        jdbc.update("UPDATE event_submission SET status = 'ACCEPTED' WHERE submission_id = ? AND status IN ('SUBMITTING', 'UNKNOWN')", id.toString());
    }
    public void uncertain(UUID id) {
        jdbc.update("UPDATE event_submission SET status = 'UNKNOWN' WHERE submission_id = ? AND status = 'SUBMITTING'", id.toString());
    }
    @Transactional
    public void process(BillingEvent event, String submissionId) {
        boolean inserted = processor.process(event);
        if (submissionId != null) {
            UUID id = UUID.fromString(submissionId);
            // Both the billing transaction and receipt transition commit together.
            // Re-delivery after commit must not turn an original processed receipt into a duplicate.
            jdbc.update("""
                UPDATE event_submission SET status = ?, completed_at = CURRENT_TIMESTAMP(6)
                WHERE submission_id = ? AND status IN ('SUBMITTING', 'ACCEPTED', 'UNKNOWN')
                """, inserted ? "PROCESSED" : "DUPLICATE", id.toString());
        }
    }
}
