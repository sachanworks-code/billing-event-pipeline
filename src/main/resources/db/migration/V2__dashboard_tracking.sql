CREATE TABLE event_submission (
    submission_id VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    event_id VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    invoice_id VARCHAR(64) NOT NULL,
    customer_id VARCHAR(64) NOT NULL,
    amount DECIMAL(14,2) NOT NULL,
    currency CHAR(3) NOT NULL,
    payload TEXT NOT NULL,
    status VARCHAR(24) NOT NULL,
    submitted_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    completed_at TIMESTAMP(6) NULL,
    INDEX idx_submission_time (submitted_at, submission_id),
    INDEX idx_submission_event (event_id)
) ENGINE=InnoDB;

CREATE TABLE failed_event (
    failure_id VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    submission_id VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,
    event_id VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,
    payload MEDIUMTEXT NOT NULL,
    reason VARCHAR(2000) NOT NULL,
    source_topic VARCHAR(255) NOT NULL,
    source_partition INT NOT NULL,
    source_offset BIGINT NOT NULL,
    failed_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_dlq_record (source_topic, source_partition, source_offset),
    INDEX idx_failure_time (failed_at, failure_id)
) ENGINE=InnoDB;
