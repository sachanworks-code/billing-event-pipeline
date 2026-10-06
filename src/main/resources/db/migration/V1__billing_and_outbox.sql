CREATE TABLE billing_event (
    event_id VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    invoice_id VARCHAR(64) NOT NULL,
    customer_id VARCHAR(64) NOT NULL,
    amount DECIMAL(14,2) NOT NULL,
    currency CHAR(3) NOT NULL,
    billing_date DATE NOT NULL,
    payload TEXT NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    CONSTRAINT positive_amount CHECK (amount > 0)
) ENGINE=InnoDB;

CREATE TABLE audit_outbox (
    event_id VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    payload TEXT NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    published_at TIMESTAMP(6) NULL,
    CONSTRAINT fk_outbox_event FOREIGN KEY (event_id) REFERENCES billing_event(event_id),
    INDEX idx_pending (published_at, created_at, event_id)
) ENGINE=InnoDB;
