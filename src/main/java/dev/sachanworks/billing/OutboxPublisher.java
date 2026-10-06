package dev.sachanworks.billing;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
@Service
public class OutboxPublisher {
    private final JdbcTemplate jdbc;
    private final AuditSender sender;
    public OutboxPublisher(JdbcTemplate jdbc, AuditSender sender) { this.jdbc = jdbc; this.sender = sender; }
    @Transactional
    public boolean publishOne() {
        var rows = jdbc.query("""
                SELECT event_id, payload FROM audit_outbox
                WHERE published_at IS NULL ORDER BY created_at, event_id LIMIT 1 FOR UPDATE SKIP LOCKED
                """, (rs, i) -> new PendingAudit(rs.getString(1), rs.getString(2)));
        if (rows.isEmpty()) return false;
        var audit = rows.getFirst();
        // Lock is held through broker acknowledgement; a failed send rolls back the transaction.
        sender.send(audit.eventId(), audit.payload());
        jdbc.update("UPDATE audit_outbox SET published_at = CURRENT_TIMESTAMP(6) WHERE event_id = ?", audit.eventId());
        return true;
    }
    private record PendingAudit(String eventId, String payload) {}
}
