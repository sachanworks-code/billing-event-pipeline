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
    public BillingController(KafkaTemplate<String, String> kafka, EventCodec codec, JdbcTemplate jdbc,
                             @Value("${billing.topics.input}") String input) {
        this.kafka = kafka; this.codec = codec; this.jdbc = jdbc; this.input = input;
    }
    @PostMapping
    public ResponseEntity<Map<String, Object>> submit(@Valid @RequestBody BillingEvent event) {
        try { kafka.send(input, event.eventId().toString(), codec.encode(event.normalized())).get(15, TimeUnit.SECONDS); }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Submission interrupted; retry with the same eventId", e);
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Broker acknowledgement unavailable; retry with the same eventId", e);
        }
        return ResponseEntity.accepted().location(URI.create("/api/v1/billing-events/" + event.eventId()))
                .body(Map.of("eventId", event.eventId(), "status", "ACCEPTED"));
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
