package dev.sachanworks.billing;

import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;

@RestController
@RequestMapping("/api/v1/dashboard")
public class DashboardController {
    private final JdbcTemplate jdbc;
    private final EventCodec codec;
    public DashboardController(JdbcTemplate jdbc, EventCodec codec) { this.jdbc = jdbc; this.codec = codec; }

    @GetMapping("/summary")
    public Map<String, Object> summary() {
        var metrics = new LinkedHashMap<String, Object>();
        metrics.put("submissions", count("SELECT COUNT(*) FROM event_submission"));
        metrics.put("billsSaved", count("SELECT COUNT(*) FROM billing_event"));
        metrics.put("auditsPending", count("SELECT COUNT(*) FROM audit_outbox WHERE published_at IS NULL"));
        metrics.put("auditsPublished", count("SELECT COUNT(*) FROM audit_outbox WHERE published_at IS NOT NULL"));
        metrics.put("failures", count("SELECT COUNT(*) FROM failed_event"));
        metrics.put("duplicates", count("SELECT COUNT(*) FROM event_submission WHERE status = 'DUPLICATE'"));
        metrics.put("processing", count("SELECT COUNT(*) FROM event_submission WHERE status IN ('SUBMITTING','ACCEPTED')"));
        metrics.put("uncertain", count("SELECT COUNT(*) FROM event_submission WHERE status = 'UNKNOWN'"));
        var activity = jdbc.query("""
            SELECT FLOOR(UNIX_TIMESTAMP(submitted_at)/300)*300000 AS bucket_ms, COUNT(*) AS total
            FROM event_submission WHERE submitted_at >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR
            GROUP BY bucket_ms ORDER BY bucket_ms
            """, (rs, i) -> Map.of("time", rs.getLong(1), "count", rs.getLong(2)));
        return Map.of("metrics", metrics, "activity", activity, "serverTime", Instant.now().toString());
    }

    @GetMapping("/submissions")
    public Map<String, Object> submissions(@RequestParam(defaultValue = "") String search,
                                           @RequestParam(defaultValue = "ALL") String status,
                                           @RequestParam(defaultValue = "0") int page) {
        validatePage(page);
        String filter = search.trim();
        if (filter.length() > 64) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Search is limited to 64 characters");
        if (!Set.of("ALL","SUBMITTING","ACCEPTED","PROCESSED","DUPLICATE","FAILED","UNKNOWN").contains(status))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unknown submission status");
        String where = " WHERE (? = 'ALL' OR s.status = ?) AND (? = '' OR LOCATE(?, s.invoice_id) > 0 OR LOCATE(?, s.customer_id) > 0 OR LOCATE(?, s.event_id) > 0)";
        Object[] args = {status,status,filter,filter,filter,filter};
        Long total = jdbc.queryForObject("SELECT COUNT(*) FROM event_submission s" + where, Long.class, args);
        var params = new ArrayList<>(Arrays.asList(args)); params.add(page * 20);
        var rows = jdbc.query("""
            SELECT s.*, UNIX_TIMESTAMP(s.submitted_at)*1000 AS submitted_ms,
            UNIX_TIMESTAMP(s.completed_at)*1000 AS completed_ms,
            UNIX_TIMESTAMP(o.published_at)*1000 AS published_ms
            FROM event_submission s LEFT JOIN audit_outbox o ON s.event_id = o.event_id
            """ + where + " ORDER BY s.submitted_at DESC, s.submission_id DESC LIMIT 20 OFFSET ?",
            (rs, i) -> submission(rs, false), params.toArray());
        return Map.of("items", rows, "total", total, "page", page, "pageSize", 20);
    }

    @GetMapping("/submissions/{id}")
    public Map<String, Object> submission(@PathVariable UUID id) {
        var rows = jdbc.query("""
            SELECT s.*, UNIX_TIMESTAMP(s.submitted_at)*1000 AS submitted_ms,
            UNIX_TIMESTAMP(s.completed_at)*1000 AS completed_ms,
            UNIX_TIMESTAMP(o.published_at)*1000 AS published_ms
            FROM event_submission s LEFT JOIN audit_outbox o ON s.event_id = o.event_id WHERE s.submission_id = ?
            """, (rs, i) -> submission(rs, true), id.toString());
        if (rows.isEmpty()) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Submission not found");
        var result = rows.getFirst();
        var failures = jdbc.query("SELECT reason FROM failed_event WHERE submission_id = ? ORDER BY failed_at DESC LIMIT 1", (rs,i) -> rs.getString(1), id.toString());
        result.put("failureReason", failures.isEmpty() ? null : failures.getFirst());
        return result;
    }

    @GetMapping("/failures")
    public Map<String, Object> failures(@RequestParam(defaultValue = "0") int page) {
        validatePage(page);
        var rows = jdbc.query("""
            SELECT failure_id, event_id, submission_id, reason, source_topic, source_partition, source_offset,
            UNIX_TIMESTAMP(failed_at)*1000 AS failed_ms FROM failed_event
            ORDER BY failed_at DESC, failure_id DESC LIMIT 20 OFFSET ?
            """, (rs,i) -> failure(rs), page * 20);
        return Map.of("items", rows, "total", count("SELECT COUNT(*) FROM failed_event"), "page", page, "pageSize", 20);
    }

    @GetMapping("/failures/{id}")
    public Map<String, Object> failure(@PathVariable UUID id) {
        var rows = jdbc.query("""
            SELECT *, UNIX_TIMESTAMP(failed_at)*1000 AS failed_ms FROM failed_event WHERE failure_id = ?
            """, (rs,i) -> { var value = failure(rs); value.put("payload", rs.getString("payload")); return value; }, id.toString());
        if (rows.isEmpty()) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Failure not found");
        return rows.getFirst();
    }

    private Map<String, Object> submission(ResultSet rs, boolean detail) throws SQLException {
        var row = new LinkedHashMap<String, Object>();
        String state = rs.getString("status");
        boolean processed = Set.of("PROCESSED", "DUPLICATE").contains(state);
        row.put("submissionId", rs.getString("submission_id")); row.put("eventId", rs.getString("event_id"));
        row.put("invoiceId", rs.getString("invoice_id")); row.put("customerId", rs.getString("customer_id"));
        row.put("amount", rs.getBigDecimal("amount")); row.put("currency", rs.getString("currency")); row.put("status", state);
        row.put("submittedAt", instant(rs,"submitted_ms")); row.put("completedAt", instant(rs,"completed_ms"));
        row.put("auditPublishedAt", processed ? instant(rs,"published_ms") : null);
        row.put("auditStatus", processed ? (rs.getObject("published_ms") == null ? "PENDING" : "PUBLISHED") : "NOT_APPLICABLE");
        if (detail) row.put("event", codec.decode(rs.getString("payload")));
        return row;
    }
    private Map<String, Object> failure(ResultSet rs) throws SQLException {
        var row = new LinkedHashMap<String, Object>();
        row.put("failureId",rs.getString("failure_id")); row.put("eventId",rs.getString("event_id"));
        row.put("submissionId",rs.getString("submission_id")); row.put("reason",rs.getString("reason"));
        row.put("failedAt",instant(rs,"failed_ms")); row.put("topic",rs.getString("source_topic"));
        row.put("partition",rs.getInt("source_partition")); row.put("offset",rs.getLong("source_offset")); return row;
    }
    private long count(String sql) { return jdbc.queryForObject(sql, Long.class); }
    private static String instant(ResultSet rs, String column) throws SQLException {
        return rs.getObject(column) == null ? null : Instant.ofEpochMilli(rs.getLong(column)).toString();
    }
    private static void validatePage(int page) {
        if (page < 0 || page > 100000) throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Invalid page");
    }
}
