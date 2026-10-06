package dev.sachanworks.billing;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
import java.util.Map;
@Service
public class BillingProcessor {
    private final JdbcTemplate jdbc;
    private final EventCodec codec;
    public BillingProcessor(JdbcTemplate jdbc, EventCodec codec) { this.jdbc = jdbc; this.codec = codec; }
    @Transactional
    public boolean process(BillingEvent event) {
        String id = event.eventId().toString();
        String payload = codec.encode(event.normalized());
        try {
            jdbc.update("""
                INSERT INTO billing_event(event_id, invoice_id, customer_id, amount, currency, billing_date, payload)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """, id, event.invoiceId(), event.customerId(), event.amount(), event.currency(), event.billingDate(), payload);
        } catch (DuplicateKeyException e) {
            // A shared locking read sees the winner without upgrading duplicate-key shared locks.
            String existing = jdbc.queryForObject("SELECT payload FROM billing_event WHERE event_id = ? FOR SHARE", String.class, id);
            if (!payload.equals(existing)) {
                throw new PermanentEventException("An eventId was reused with a different payload");
            }
            return false;
        }
        String audit = codec.encode(Map.of("auditId", id, "eventId", id, "invoiceId", event.invoiceId(),
                "type", "BILLING_PERSISTED", "recordedAt", Instant.now().toString()));
        jdbc.update("INSERT INTO audit_outbox(event_id, payload) VALUES (?, ?)", id, audit);
        return true;
    }
}
