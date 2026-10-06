package dev.sachanworks.billing;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
@Component
public class BillingListener {
    private final EventCodec codec;
    private final BillingProcessor processor;
    public BillingListener(EventCodec codec, BillingProcessor processor) { this.codec = codec; this.processor = processor; }
    @KafkaListener(topics = "${billing.topics.input}", groupId = "billing-processor")
    public void receive(String json) { processor.process(codec.decode(json)); }
}
