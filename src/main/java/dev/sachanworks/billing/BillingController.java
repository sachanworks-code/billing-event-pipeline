package dev.sachanworks.billing;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;
import java.net.URI;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
@RestController
@RequestMapping("/api/v1/billing-events")
public class BillingController {
    private final KafkaTemplate<String, String> kafka;
    private final EventCodec codec;
    private final JdbcTemplate jdbc;
    private final String input;
    private final SubmissionTracker tracker;
    public BillingController(KafkaTemplate<String, String> kafka, EventCodec codec, JdbcTemplate jdbc,
                             @Value("${billing.topics.input}") String input, SubmissionTracker tracker) {
        this.kafka = kafka; this.codec = codec; this.jdbc = jdbc; this.input = input; this.tracker = tracker;
    }
    @PostMapping
    public ResponseEntity<Map<String, Object>> submit(@Valid @RequestBody BillingEvent event) {
        UUID submissionId = tracker.begin(event);
        var record = new org.apache.kafka.clients.producer.ProducerRecord<String, String>(input, event.eventId().toString(), codec.encode(event.normalized()));
        record.headers().add(SubmissionTracker.HEADER, submissionId.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        try { kafka.send(record).get(15, TimeUnit.SECONDS); }

        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            tracker.uncertain(submissionId);
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Submission interrupted; retry with the same eventId", e);
        } catch (Exception e) {
            tracker.uncertain(submissionId);
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Broker acknowledgement unavailable; retry with the same eventId", e);
        }
        tracker.accepted(submissionId);
        return ResponseEntity.accepted().location(URI.create("/api/v1/billing-events/" + event.eventId()))
                .body(Map.of("eventId", event.eventId(), "submissionId", submissionId, "status", "ACCEPTED"));
    }
    @GetMapping("/{eventId}")
    public Map<String, Object> find(@PathVariable UUID eventId) {
        var result = jdbc.query("""
                SELECT b.payload, o.published_at FROM billing_event b
                JOIN audit_outbox o ON b.event_id = o.event_id WHERE b.event_id = ?
                """, (rs, i) -> Map.<String, Object>of("event", codec.decode(rs.getString(1)),
                "status", "PERSISTED", "auditStatus", rs.getTimestamp(2) == null ? "PENDING" : "PUBLISHED"), eventId.toString());
        if (result.isEmpty()) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Event has not been persisted");
        return result.getFirst();
    }
}
