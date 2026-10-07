package dev.sachanworks.billing;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.kafka.KafkaContainer;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"billing.outbox.enabled=false", "billing.retry.delay-ms=100"})
class PipelineIT {
    @Container static final MySQLContainer mysql = new MySQLContainer("mysql:8.4");
    @Container static final KafkaContainer kafka = new KafkaContainer("apache/kafka:3.9.1");
    @DynamicPropertySource static void config(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", mysql::getJdbcUrl);
        r.add("spring.datasource.username", mysql::getUsername);
        r.add("spring.datasource.password", mysql::getPassword);
        r.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        r.add("billing.api-key", () -> "test-api-key");
    }
    @Autowired TestRestTemplate http;
    @Autowired JdbcTemplate jdbc;
    @Autowired EventCodec codec;
    @Autowired OutboxPublisher publisher;
    @Autowired KafkaTemplate<String, String> template;
    @MockitoSpyBean BillingProcessor processor;
    @MockitoSpyBean KafkaAuditSender sender;

    @BeforeEach void clean() {
        jdbc.execute("DELETE FROM audit_outbox");
        jdbc.execute("DELETE FROM billing_event");
    }
    BillingEvent event() {
        return new BillingEvent(UUID.randomUUID(), "INV-DEMO", "CUSTOMER-DEMO", new BigDecimal("1499.00"), "INR", LocalDate.of(2026, 10, 1));
    }
    int count(String table, UUID id) {
        return jdbc.queryForObject("SELECT COUNT(*) FROM " + table + " WHERE event_id = ?", Integer.class, id.toString());
    }
    void send(BillingEvent e) throws Exception { template.send("billing.events.v1", e.eventId().toString(), codec.encode(e)).get(10, TimeUnit.SECONDS); }
    void persisted(BillingEvent e) { await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(count("billing_event", e.eventId())).isEqualTo(1)); }
    KafkaConsumer<String, String> consumer(String topic) {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        var c = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
        c.subscribe(List.of(topic));
        return c;
    }
    List<String> messages(KafkaConsumer<String, String> c, String marker, int expected) {
        var found = new ArrayList<String>();
        await().atMost(Duration.ofSeconds(25)).until(() -> {
            c.poll(Duration.ofMillis(250)).forEach(r -> { if (r.value().contains(marker)) found.add(r.value()); });
            return found.size() >= expected;
        });
        return found;
    }
    HttpHeaders apiHeaders() {
        var headers = new HttpHeaders();
        headers.set("X-API-Key", "test-api-key");
        return headers;
    }
    void adminSql(String sql) {
        try (var connection = java.sql.DriverManager.getConnection(mysql.getJdbcUrl(), "root", mysql.getPassword());
             var statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (java.sql.SQLException e) { throw new IllegalStateException("Failure injection setup failed", e); }
    }
    @Test void httpToKafkaToMysqlToAuditAndDuplicateReplay() throws Exception {
        var e = event();
        try (var audit = consumer("billing.audit.v1")) {
            var response = http.exchange("/api/v1/billing-events", HttpMethod.POST, new HttpEntity<>(e, apiHeaders()), Map.class);
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
            persisted(e);
            send(e);
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> verify(processor, times(2)).process(e));
            assertThat(count("billing_event", e.eventId())).isEqualTo(1);
            assertThat(count("audit_outbox", e.eventId())).isEqualTo(1);
            assertThat(publisher.publishOne()).isTrue();
            assertThat(publisher.publishOne()).isFalse();
            assertThat(messages(audit, e.eventId().toString(), 1).getFirst()).contains("BILLING_PERSISTED");
            var read = http.exchange("/api/v1/billing-events/" + e.eventId(), HttpMethod.GET, new HttpEntity<>(apiHeaders()), Map.class);
            assertThat(read.getStatusCode()).isEqualTo(HttpStatus.OK);
            assertThat(read.getBody()).containsEntry("status", "PERSISTED").containsEntry("auditStatus", "PUBLISHED");
        }
    }
    @Test void changedPayloadForSameIdGoesToDlqWithoutChangingTheBill() throws Exception {
        var e = event();
        processor.process(e);
        var conflicting = new BillingEvent(e.eventId(), e.invoiceId(), e.customerId(), new BigDecimal("999.00"), e.currency(), e.billingDate());
        try (var dlq = consumer("billing.events.v1.dlq")) {
            send(conflicting);
            assertThat(messages(dlq, e.eventId().toString(), 1)).hasSize(1);
            assertThat(jdbc.queryForObject("SELECT amount FROM billing_event WHERE event_id = ?", BigDecimal.class, e.eventId().toString())).isEqualByComparingTo("1499.00");
            assertThat(count("audit_outbox", e.eventId())).isEqualTo(1);
        }
    }
    @Test void malformedJsonIsDeadLettered() throws Exception {
        String marker = "malformed-" + UUID.randomUUID();
        try (var dlq = consumer("billing.events.v1.dlq")) {
            template.send("billing.events.v1", marker, marker).get(10, TimeUnit.SECONDS);
            assertThat(messages(dlq, marker, 1)).containsExactly(marker);
        }
    }
    @Test void transientProcessingFailureRetriesAndThenPersists() throws Exception {
        var e = event();
        var attempts = new AtomicInteger();
        doAnswer(invocation -> {
            if (attempts.incrementAndGet() < 3) throw new TransientDataAccessResourceException("simulated transient database outage");
            return invocation.callRealMethod();
        }).when(processor).process(argThat(x -> x != null && x.eventId().equals(e.eventId())));
        send(e);
        persisted(e);
        assertThat(attempts.get()).isEqualTo(3);
        assertThat(count("audit_outbox", e.eventId())).isEqualTo(1);
    }
    @Test void exhaustedRetriesGoToDlqWithoutPersisting() throws Exception {
        var e = event();
        var attempts = new AtomicInteger();
        doAnswer(invocation -> {
            attempts.incrementAndGet();
            throw new TransientDataAccessResourceException("simulated persistent database outage");
        }).when(processor).process(argThat(x -> x != null && x.eventId().equals(e.eventId())));
        try (var dlq = consumer("billing.events.v1.dlq")) {
            send(e);
            messages(dlq, e.eventId().toString(), 1);
            assertThat(attempts.get()).isEqualTo(3);
            assertThat(count("billing_event", e.eventId())).isZero();
            assertThat(count("audit_outbox", e.eventId())).isZero();
        }
    }
    @Test void failedAuditSendLeavesPendingOutboxAndLaterSucceeds() {
        var e = event();
        processor.process(e);
        doThrow(new IllegalStateException("simulated broker outage")).doCallRealMethod().when(sender).send(eq(e.eventId().toString()), anyString());
        assertThatThrownBy(publisher::publishOne).isInstanceOf(IllegalStateException.class);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM audit_outbox WHERE published_at IS NULL", Integer.class)).isEqualTo(1);
        assertThat(publisher.publishOne()).isTrue();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM audit_outbox WHERE published_at IS NULL", Integer.class)).isZero();
    }
    @Test void outboxInsertFailureRollsBackTheBillingRecord() {
        var e = event();
        adminSql("CREATE TRIGGER reject_audit BEFORE INSERT ON audit_outbox FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='simulated outbox failure'");
        try {
            assertThatThrownBy(() -> processor.process(e)).isInstanceOf(DataAccessException.class);
            assertThat(count("billing_event", e.eventId())).isZero();
        } finally { adminSql("DROP TRIGGER reject_audit"); }
    }
    @Test void concurrentDuplicateDeliveriesCreateOneBillAndOneOutboxRow() throws Exception {
        var e = event();
        try (var executor = Executors.newFixedThreadPool(8)) {
            var barrier = new CyclicBarrier(8);
            var tasks = new ArrayList<Callable<Boolean>>();
            for (int i = 0; i < 8; i++) tasks.add(() -> { barrier.await(10, TimeUnit.SECONDS); return processor.process(e); });
            int inserted = 0;
            for (var f : executor.invokeAll(tasks)) if (f.get(15, TimeUnit.SECONDS)) inserted++;
            assertThat(inserted).isEqualTo(1);
            assertThat(count("billing_event", e.eventId())).isEqualTo(1);
            assertThat(count("audit_outbox", e.eventId())).isEqualTo(1);
        }
    }
    @Test void crashAfterBrokerAckCanReplayAuditWithTheSameStableId() {
        var e = event();
        processor.process(e);
        try (var audit = consumer("billing.audit.v1")) {
            adminSql("CREATE TRIGGER reject_ack BEFORE UPDATE ON audit_outbox FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='simulated crash after broker ack'");
            try { assertThatThrownBy(publisher::publishOne).isInstanceOf(DataAccessException.class); }
            finally { adminSql("DROP TRIGGER reject_ack"); }
            assertThat(publisher.publishOne()).isTrue();
            assertThat(messages(audit, e.eventId().toString(), 2)).hasSize(2).allSatisfy(payload -> assertThat(payload).contains(e.eventId().toString()));
        }
    }
}
