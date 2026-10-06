package dev.sachanworks.billing;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import java.util.concurrent.TimeUnit;
@Component
public class KafkaAuditSender implements AuditSender {
    private final KafkaTemplate<String, String> kafka;
    private final String topic;
    public KafkaAuditSender(KafkaTemplate<String, String> kafka, @Value("${billing.topics.audit}") String topic) {
        this.kafka = kafka; this.topic = topic;
    }
    public void send(String eventId, String payload) {
        try { kafka.send(topic, eventId, payload).get(15, TimeUnit.SECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException("Audit publication interrupted", e); }
        catch (Exception e) { throw new IllegalStateException("Audit publication failed", e); }
    }
}
