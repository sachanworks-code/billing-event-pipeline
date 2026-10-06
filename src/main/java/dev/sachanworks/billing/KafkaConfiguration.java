package dev.sachanworks.billing;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.KafkaAdmin.NewTopics;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import java.time.Duration;
@Configuration
public class KafkaConfiguration {
    @Bean
    NewTopics topics(@Value("${billing.topics.input}") String input,
                     @Value("${billing.topics.audit}") String audit,
                     @Value("${billing.topics.dlq}") String dlq) {
        return new NewTopics(TopicBuilder.name(input).partitions(3).replicas(1).build(),
                TopicBuilder.name(audit).partitions(3).replicas(1).build(),
                TopicBuilder.name(dlq).partitions(3).replicas(1).build());
    }
    @Bean
    DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template,
                                    @Value("${billing.topics.dlq}") String dlq,
                                    @Value("${billing.retry.delay-ms}") long delay,
                                    @Value("${billing.retry.max-retries}") long retries) {
        var recoverer = new DeadLetterPublishingRecoverer(template,
                (record, exception) -> new TopicPartition(dlq, record.partition()));
        recoverer.setFailIfSendResultIsError(true);
        recoverer.setWaitForSendResultTimeout(Duration.ofSeconds(15));
        var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(delay, retries));
        handler.addNotRetryableExceptions(PermanentEventException.class);
        return handler;
    }
}
