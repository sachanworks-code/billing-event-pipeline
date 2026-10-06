package dev.sachanworks.billing;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.validation.Validation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.UUID;
import static org.assertj.core.api.Assertions.*;
class EventCodecTest {
    private final EventCodec codec = new EventCodec(JsonMapper.builder().addModule(new JavaTimeModule()).build(),
            Validation.buildDefaultValidatorFactory().getValidator());
    private final BillingEvent valid = new BillingEvent(UUID.randomUUID(), "INV-1", "CUSTOMER-1",
            new BigDecimal("12.50"), "INR", LocalDate.of(2026, 10, 1));
    @Test void roundTripsTheContract() { assertThat(codec.decode(codec.encode(valid))).isEqualTo(valid); }
    @Test void normalizesEquivalentAmounts() {
        assertThat(codec.decode(codec.encode(valid).replace("12.50", "12.5")).amount()).isEqualTo(new BigDecimal("12.50"));
    }
    @ParameterizedTest @ValueSource(strings = {"null", "{}", "not-json", "[]"})
    void rejectsMalformedOrMissingFields(String input) { assertThatThrownBy(() -> codec.decode(input)).isInstanceOf(PermanentEventException.class); }
    @ParameterizedTest @ValueSource(strings = {"0", "-10", "1.001", "1000000000000"})
    void rejectsInvalidAmounts(String amount) {
        String json = codec.encode(valid).replace("12.50", amount);
        assertThatThrownBy(() -> codec.decode(json)).isInstanceOf(PermanentEventException.class);
    }
    @Test void rejectsInvalidCurrency() {
        assertThatThrownBy(() -> codec.decode(codec.encode(valid).replace("INR", "inr"))).isInstanceOf(PermanentEventException.class);
    }
    @Test void rejectsUnknownFields() {
        String json = codec.encode(valid).replace("}", ",\"unexpected\":true}");
        assertThatThrownBy(() -> codec.decode(json)).isInstanceOf(PermanentEventException.class);
    }
}
