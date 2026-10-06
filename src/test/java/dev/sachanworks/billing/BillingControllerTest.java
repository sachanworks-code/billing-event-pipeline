package dev.sachanworks.billing;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import java.util.concurrent.CompletableFuture;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
@WebMvcTest(BillingController.class)
@Import(EventCodec.class)
class BillingControllerTest {
    @Autowired MockMvc mvc;
    @MockitoBean KafkaTemplate<String, String> kafka;
    @MockitoBean JdbcTemplate jdbc;
    private String input() throws Exception { return Files.readString(Path.of("examples/billing-event.json")); }
    @Test void acceptsOnlyAfterBrokerAcknowledges() throws Exception {
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(CompletableFuture.completedFuture(null));
        mvc.perform(post("/api/v1/billing-events").contentType("application/json").content(input()))
                .andExpect(status().isAccepted()).andExpect(jsonPath("$.status").value("ACCEPTED"))
                .andExpect(header().string("Location", "/api/v1/billing-events/54ea3148-3f35-43e8-a003-668e934cac01"));
    }
    @Test void invalidAmountDoesNotReachKafka() throws Exception {
        mvc.perform(post("/api/v1/billing-events").contentType("application/json").content(input().replace("1499.00", "-1")))
                .andExpect(status().isBadRequest());
        verify(kafka, never()).send(anyString(), anyString(), anyString());
    }
    @Test void invalidIdDoesNotReachKafka() throws Exception {
        mvc.perform(post("/api/v1/billing-events").contentType("application/json").content(input().replace("54ea3148-3f35-43e8-a003-668e934cac01", "bad-id")))
                .andExpect(status().isBadRequest());
        verify(kafka, never()).send(anyString(), anyString(), anyString());
    }
    @Test void brokerFailureReturnsUnavailable() throws Exception {
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker offline")));
        mvc.perform(post("/api/v1/billing-events").contentType("application/json").content(input())).andExpect(status().isServiceUnavailable());
    }
    @Test void invalidLookupIdReturnsBadRequest() throws Exception {
        mvc.perform(get("/api/v1/billing-events/not-a-uuid")).andExpect(status().isBadRequest());
    }
}
