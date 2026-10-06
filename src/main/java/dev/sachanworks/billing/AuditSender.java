package dev.sachanworks.billing;
public interface AuditSender { void send(String eventId, String payload); }
