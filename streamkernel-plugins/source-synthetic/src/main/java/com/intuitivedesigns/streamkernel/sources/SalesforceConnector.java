/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.sources;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.core.SourceConnector;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Production-Grade Salesforce Source.
 * Features:
 * - OAuth 2.0 Authentication (with Auto-Refresh on 401)
 * - SOQL Polling with Incremental Watermark (LastModifiedDate)
 * - Zero-Dependency JSON Splitting
 */
public final class SalesforceConnector implements SourceConnector<String> {

    private static final Logger log = LoggerFactory.getLogger(SalesforceConnector.class);

    // Config Keys
    private static final String CFG_LOGIN_URL = "source.salesforce.login.url";
    private static final String CFG_CLIENT_ID = "source.salesforce.client.id";
    private static final String CFG_CLIENT_SECRET = "source.salesforce.client.secret";
    private static final String CFG_USERNAME = "source.salesforce.username";
    private static final String CFG_PASSWORD = "source.salesforce.password"; // Password + Security Token
    private static final String CFG_SOQL = "source.salesforce.soql";
    private static final String CFG_POLL_MS = "source.salesforce.poll.interval.ms";

    // Defaults
    private static final String DEFAULT_LOGIN_URL = "https://login.salesforce.com";
    private static final long DEFAULT_POLL_MS = 10_000L;

    private final HttpClient client;
    private final MetricsRuntime metrics;

    // Auth State
    private final String loginUrl;
    private final String clientId;
    private final String clientSecret;
    private final String username;
    private final String password;

    private volatile String accessToken;
    private volatile String instanceUrl;

    // Polling State
    private final String baseSoql;
    private final Duration pollInterval;
    private Instant nextPollTime = Instant.now();
    private Instant lastWatermark = Instant.now().minus(Duration.ofDays(1)); // Default lookback 1 day

    private final Queue<PipelinePayload<String>> buffer = new ConcurrentLinkedQueue<>();

    // Patterns for dependency-free parsing
    private static final Pattern PATTERN_ACCESS_TOKEN = Pattern.compile("\"access_token\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern PATTERN_INSTANCE_URL = Pattern.compile("\"instance_url\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern PATTERN_RECORDS_ARRAY = Pattern.compile("\"records\"\\s*:\\s*\\[(.*)]\\s*,\\s*\"totalSize\"", Pattern.DOTALL);

    private SalesforceConnector(String loginUrl, String clientId, String clientSecret,
                                String username, String password, String soql,
                                Duration pollInterval, MetricsRuntime metrics) {
        this.loginUrl = loginUrl;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.username = username;
        this.password = password;
        this.baseSoql = soql;
        this.pollInterval = pollInterval;
        this.metrics = metrics;

        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .version(HttpClient.Version.HTTP_2)
                .build();
    }

    public static SalesforceConnector fromConfig(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");

        return new SalesforceConnector(
                config.getString(CFG_LOGIN_URL, DEFAULT_LOGIN_URL),
                require(config, CFG_CLIENT_ID),
                require(config, CFG_CLIENT_SECRET),
                require(config, CFG_USERNAME),
                require(config, CFG_PASSWORD),
                require(config, CFG_SOQL),
                Duration.ofMillis(config.getLong(CFG_POLL_MS, DEFAULT_POLL_MS)),
                metrics
        );
    }

    @Override
    public void connect() {
        log.info("🔌 Connecting to Salesforce (User: {})...", username);
        authenticate(); // Fail fast if creds are wrong
    }

    @Override
    public void disconnect() {
        log.info("🔌 Disconnecting Salesforce Connector.");
        // Token revocation logic could go here if strict security is needed
    }

    @Override
    public PipelinePayload<String> fetch() {
        // 1. Drain Buffer
        if (!buffer.isEmpty()) return buffer.poll();

        // 2. Poll Wait
        if (Instant.now().isBefore(nextPollTime)) return null;

        // 3. Execute Poll
        try {
            pollSalesforce();
            nextPollTime = Instant.now().plus(pollInterval);
        } catch (Exception e) {
            log.error("Salesforce Poll Failed", e);
            metrics.counter("source.salesforce.errors", 1.0);
            // Backoff slightly on error
            nextPollTime = Instant.now().plusSeconds(30);
        }

        return buffer.poll();
    }

    private void pollSalesforce() throws Exception {
        if (accessToken == null) authenticate();

        // Inject Incremental Watermark
        // Query must act like: SELECT Id, Name FROM Account WHERE LastModifiedDate > :watermark
        String incrementalSoql = appendWatermark(baseSoql, lastWatermark);
        String queryUrl = instanceUrl + "/services/data/v57.0/query?q=" + encode(incrementalSoql);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(queryUrl))
                .header("Authorization", "Bearer " + accessToken)
                .header("Accept", "application/json")
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        // Handle Token Expiry (401)
        if (response.statusCode() == 401) {
            log.warn("Salesforce Token Expired (401). Refreshing...");
            authenticate(); // Refresh
            pollSalesforce(); // Retry once
            return;
        }

        if (response.statusCode() != 200) {
            throw new RuntimeException("SOQL Query Failed: " + response.statusCode() + " Body: " + response.body());
        }

        List<String> records = parseRecords(response.body());
        if (!records.isEmpty()) {
            log.info("Fetched {} records from Salesforce.", records.size());
            for (String rec : records) {
                // Generate a deterministic ID if possible, otherwise UUID
                String id = extractId(rec);
                buffer.add(new PipelinePayload<>(id, rec, Instant.now(), null));
            }

            // Advance watermark
            lastWatermark = Instant.now();
            metrics.counter("source.salesforce.records", records.size());
        }
    }

    private void authenticate() {
        try {
            // OAuth 2.0 Password Flow
            String body = "grant_type=password" +
                    "&client_id=" + encode(clientId) +
                    "&client_secret=" + encode(clientSecret) +
                    "&username=" + encode(username) +
                    "&password=" + encode(password);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(loginUrl + "/services/oauth2/token"))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new RuntimeException("Auth Failed: " + response.statusCode() + " " + response.body());
            }

            String json = response.body();
            this.accessToken = extractJsonValue(json, PATTERN_ACCESS_TOKEN);
            this.instanceUrl = extractJsonValue(json, PATTERN_INSTANCE_URL);

            if (accessToken == null || instanceUrl == null) {
                throw new RuntimeException("Failed to parse Access Token or Instance URL");
            }

            log.info("✅ Salesforce Authenticated. Instance: {}", instanceUrl);

        } catch (Exception e) {
            throw new RuntimeException("Salesforce Authentication Failed", e);
        }
    }

    // --- Helpers ---

    private String appendWatermark(String soql, Instant watermark) {
        String isoDate = DateTimeFormatter.ISO_INSTANT.format(watermark);
        // Basic SQL injection: append " AND LastModifiedDate > ..."
        // Note: Assumes the original SOQL has a WHERE clause. If not, needs " WHERE ...".
        // A production parser would be robust, here we use a simple heuristic:
        String operator = soql.toUpperCase().contains(" WHERE ") ? " AND " : " WHERE ";
        return soql + operator + "LastModifiedDate > " + isoDate;
    }

    /**
     * Extracts records from the JSON response without a heavy library.
     * Salesforce returns: { "totalSize": 5, "done": true, "records": [ { ... }, { ... } ] }
     */
    private List<String> parseRecords(String json) {
        List<String> list = new ArrayList<>();

        // Find the "records": [ ... ] block
        Matcher m = PATTERN_RECORDS_ARRAY.matcher(json);
        if (m.find()) {
            String arrayContent = m.group(1);

            // Split by "attributes" which appears in every Salesforce record
            // This is a "Poor Man's Splitter" - robust enough for simple flat records.
            // For complex nested structures, use a real JSON parser.
            String[] rawSplits = arrayContent.split("\\},\\s*\\{");

            for (String s : rawSplits) {
                // Clean up braces lost during split
                String clean = s.trim();
                if (!clean.startsWith("{")) clean = "{" + clean;
                if (!clean.endsWith("}")) clean = clean + "}";
                list.add(clean);
            }
        }
        return list;
    }

    private String extractId(String jsonRecord) {
        // fast extract "Id":"001..."
        int idx = jsonRecord.indexOf("\"Id\"");
        if (idx > 0) {
            int start = jsonRecord.indexOf("\"", idx + 5);
            int end = jsonRecord.indexOf("\"", start + 1);
            if (start > 0 && end > start) {
                return jsonRecord.substring(start + 1, end);
            }
        }
        return UUID.randomUUID().toString();
    }

    private String extractJsonValue(String json, Pattern pattern) {
        Matcher m = pattern.matcher(json);
        return m.find() ? m.group(1) : null;
    }

    private String encode(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    private static String require(PipelineConfig cfg, String key) {
        String v = cfg.getString(key, null);
        if (v == null) throw new IllegalArgumentException("Missing Salesforce config: " + key);
        return v;
    }
}