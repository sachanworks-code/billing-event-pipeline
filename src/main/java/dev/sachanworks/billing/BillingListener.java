package dev.sachanworks.billing;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
@Component
public class BillingListener {
    private final EventCodec codec;
    private final SubmissionTracker tracker;
    public BillingListener(EventCodec codec, SubmissionTracker tracker) { this.codec = codec; this.tracker = tracker; }
    @KafkaListener(topics = "${billing.topics.input}", groupId = "billing-processor")
    public void receive(String json, @org.springframework.messaging.handler.annotation.Header(name = SubmissionTracker.HEADER, required = false) String submissionId) {
        tracker.process(codec.decode(json), submissionId);
    }
}
