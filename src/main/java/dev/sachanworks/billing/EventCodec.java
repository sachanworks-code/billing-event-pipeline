package dev.sachanworks.billing;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Validator;
import org.springframework.stereotype.Component;
@Component
public class EventCodec {
    private final ObjectMapper mapper;
    private final Validator validator;
    public EventCodec(ObjectMapper mapper, Validator validator) { this.mapper = mapper; this.validator = validator; }
    public BillingEvent decode(String json) {
        try {
            BillingEvent event = mapper.readValue(json, BillingEvent.class);
            if (event == null || !validator.validate(event).isEmpty()) {
                throw new PermanentEventException("Billing event violates the input contract");
            }
            return event.normalized();
        } catch (JsonProcessingException e) {
            throw new PermanentEventException("Malformed billing event JSON", e);
        }
    }
    public String encode(Object value) {
        try { return mapper.writeValueAsString(value); }
        catch (JsonProcessingException e) { throw new IllegalStateException("Cannot encode event", e); }
    }
}
