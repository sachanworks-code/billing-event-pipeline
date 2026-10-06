package dev.sachanworks.billing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
@Component
@ConditionalOnProperty(name = "billing.outbox.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxSchedule {
    private static final Logger log = LoggerFactory.getLogger(OutboxSchedule.class);
    private final OutboxPublisher publisher;
    public OutboxSchedule(OutboxPublisher publisher) { this.publisher = publisher; }
    @Scheduled(fixedDelayString = "${billing.outbox.poll-ms}")
    public void publish() {
        try { for (int i = 0; i < 20 && publisher.publishOne(); i++) { /* bounded batch */ } }
        catch (RuntimeException e) { log.warn("Outbox publication failed; pending event will be retried next cycle", e); }
    }
}
