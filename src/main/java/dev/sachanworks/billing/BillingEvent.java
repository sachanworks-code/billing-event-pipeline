package dev.sachanworks.billing;
import jakarta.validation.constraints.*;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.UUID;
public record BillingEvent(
        @NotNull UUID eventId,
        @NotBlank @Size(max = 64) String invoiceId,
        @NotBlank @Size(max = 64) String customerId,
        @NotNull @DecimalMin("0.01") @Digits(integer = 12, fraction = 2) BigDecimal amount,
        @NotNull @Pattern(regexp = "[A-Z]{3}") String currency,
        @NotNull LocalDate billingDate) {
    public BillingEvent normalized() {
        return new BillingEvent(eventId, invoiceId, customerId, amount.setScale(2), currency, billingDate);
    }
}
