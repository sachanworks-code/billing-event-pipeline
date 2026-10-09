package dev.sachanworks.billing;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Component
public class FailedEventListener {
    private final JdbcTemplate jdbc;
    private final EventCodec codec;
    public FailedEventListener(JdbcTemplate jdbc, EventCodec codec) { this.jdbc = jdbc; this.codec = codec; }
    @KafkaListener(topics = "${billing.topics.dlq}", groupId = "billing-dashboard-failures", containerFactory = "failureListenerFactory")
    @Transactional
    public void receive(ConsumerRecord<String, String> record) {
        String identity = record.topic() + ":" + record.partition() + ":" + record.offset();
        String failureId = UUID.nameUUIDFromBytes(identity.getBytes(StandardCharsets.UTF_8)).toString();
        String submissionId = validUuid(header(record, SubmissionTracker.HEADER));
        String eventId = null;
        try { eventId = codec.decode(record.value()).eventId().toString(); }
        catch (PermanentEventException ignored) { /* malformed events still belong in the failure view */ }
        String reason = header(record, "billing-failure-reason");
        if (reason == null) reason = header(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE);
        if (reason == null || reason.isBlank()) reason = "Event processing failed; inspect the original payload.";
        if (reason.length() > 2000) reason = reason.substring(0, 2000);
        jdbc.update("""
            INSERT INTO failed_event(failure_id, submission_id, event_id, payload, reason, source_topic, source_partition, source_offset)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE failure_id = failure_id
            """, failureId, submissionId, eventId, record.value() == null ? "null" : record.value(), reason,
            record.topic(), record.partition(), record.offset());
        if (submissionId != null) jdbc.update("""
            UPDATE event_submission SET status = 'FAILED', completed_at = CURRENT_TIMESTAMP(6)
            WHERE submission_id = ? AND status IN ('SUBMITTING', 'ACCEPTED', 'UNKNOWN')
            """, submissionId);
    }
    private static String header(ConsumerRecord<?, ?> record, String key) {
        Header h = record.headers().lastHeader(key);
        return h == null || h.value() == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
    private static String validUuid(String value) {
        try { return value == null ? null : UUID.fromString(value).toString(); }
        catch (IllegalArgumentException ignored) { return null; }
    }
}
