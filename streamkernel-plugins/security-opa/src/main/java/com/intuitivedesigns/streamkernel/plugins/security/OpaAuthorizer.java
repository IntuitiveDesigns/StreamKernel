/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * High-performance, dependency-free OPA Client.
 * Designed for Java 21+ Virtual Threads (blocking I/O).
 *
 * Fail-closed by default: any error, timeout, or non-200 response returns false.
 */
public final class OpaAuthorizer {

    private static final Logger log = LoggerFactory.getLogger(OpaAuthorizer.class);

    private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofMillis(300);
    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofMillis(200);

    // Pre-computed headers
    private static final String HEADER_CONTENT_TYPE = "Content-Type";
    private static final String MIME_JSON = "application/json";
    private static final String HEADER_ACCEPT = "Accept";

    // Log throttling
    private static final long ERROR_LOG_THROTTLE_MS = 5_000L;
    private final AtomicLong lastErrorLogAtMs = new AtomicLong(0L);

    private final HttpClient client;
    private final URI opaUri;
    private final Duration requestTimeout;

    public OpaAuthorizer(String opaUrl) {
        this(opaUrl, DEFAULT_CONNECT_TIMEOUT, DEFAULT_REQUEST_TIMEOUT);
    }

    public OpaAuthorizer(String opaUrl, Duration connectTimeout, Duration requestTimeout) {
        final String url = normalizeRequired(opaUrl, "opaUrl");
        this.opaUri = URI.create(url);

        final Duration ct = sanitizePositive(connectTimeout, DEFAULT_CONNECT_TIMEOUT);
        this.requestTimeout = sanitizePositive(requestTimeout, DEFAULT_REQUEST_TIMEOUT);

        this.client = HttpClient.newBuilder()
                .connectTimeout(ct)
                .version(HttpClient.Version.HTTP_1_1)
                .build();
    }

    public boolean isAllowed(String user, String action, String resource) {
        final String body = buildBody(user, action, resource);

        try {
            final HttpRequest request = HttpRequest.newBuilder()
                    .uri(opaUri)
                    .timeout(requestTimeout)
                    .header(HEADER_CONTENT_TYPE, MIME_JSON)
                    .header(HEADER_ACCEPT, MIME_JSON)
                    .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                    .build();

            // Blocking call is fine (and preferred) on Virtual Threads
            final HttpResponse<String> response =
                    client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

            final int code = response.statusCode();
            if (code != 200) {
                if (log.isDebugEnabled()) {
                    log.debug("OPA non-200 (fail-closed). status={} body={}", code, response.body());
                } else {
                    throttledError("OPA non-200 (fail-closed). status={}", code);
                }
                return false;
            }

            return parseOpaResult(response.body());

        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throttledError("OPA interrupted (fail-closed). msg={}", ie.getMessage());
            return false;
        } catch (Exception e) {
            throttledError("OPA check error (fail-closed). msg={}", e.getMessage());
            return false;
        }
    }

    private void throttledError(String fmt, Object arg) {
        final long now = System.currentTimeMillis();
        final long last = lastErrorLogAtMs.get();
        if (now - last >= ERROR_LOG_THROTTLE_MS && lastErrorLogAtMs.compareAndSet(last, now)) {
            log.error(fmt, arg);
        }
    }

    private void throttledError(String fmt, Object a, Object b) {
        final long now = System.currentTimeMillis();
        final long last = lastErrorLogAtMs.get();
        if (now - last >= ERROR_LOG_THROTTLE_MS && lastErrorLogAtMs.compareAndSet(last, now)) {
            log.error(fmt, a, b);
        }
    }

    private static String buildBody(String user, String action, String resource) {
        // Base JSON chars ~64 + lengths
        final int size = 64
                + (user == null ? 0 : user.length())
                + (action == null ? 0 : action.length())
                + (resource == null ? 0 : resource.length());

        final StringBuilder sb = new StringBuilder(size);
        sb.append("{\"input\":{\"user\":\"");
        escapeJson(sb, user);
        sb.append("\",\"action\":\"");
        escapeJson(sb, action);
        sb.append("\",\"resource\":\"");
        escapeJson(sb, resource);
        sb.append("\"}}");
        return sb.toString();
    }

    /**
     * Dependency-free JSON parser optimized for OPA responses.
     * Handles: {"result": true} AND {"result": {"allow": true}}
     */
    static boolean parseOpaResult(String body) {
        if (body == null || body.isEmpty()) return false;

        final int idx = body.indexOf("\"result\"");
        if (idx < 0) return false;

        final int colon = body.indexOf(':', idx + 8);
        if (colon < 0) return false;

        int i = colon + 1;
        while (i < body.length() && isWhitespace(body.charAt(i))) i++;

        // Direct boolean: {"result": true}
        if (regionMatchesTrue(body, i)) return true;
        if (regionMatchesFalse(body, i)) return false;

        // Nested object: {"result": { ... "allow": true ... }}
        if (i < body.length() && body.charAt(i) == '{') {
            final int allowKey = body.indexOf("\"allow\"", i);
            if (allowKey > 0) {
                final int allowColon = body.indexOf(':', allowKey + 7);
                if (allowColon > 0) {
                    int j = allowColon + 1;
                    while (j < body.length() && isWhitespace(body.charAt(j))) j++;
                    return regionMatchesTrue(body, j);
                }
            }
        }

        return false;
    }

    private static boolean regionMatchesTrue(String s, int i) {
        return i >= 0 && i + 4 <= s.length() && s.regionMatches(true, i, "true", 0, 4);
    }

    private static boolean regionMatchesFalse(String s, int i) {
        return i >= 0 && i + 5 <= s.length() && s.regionMatches(true, i, "false", 0, 5);
    }

    /**
     * Optimized JSON escaper.
     */
    static void escapeJson(StringBuilder b, String s) {
        if (s == null) return;
        final int len = s.length();
        for (int i = 0; i < len; i++) {
            final char c = s.charAt(i);
            switch (c) {
                case '"':  b.append("\\\""); break;
                case '\\': b.append("\\\\"); break;
                case '\b': b.append("\\b"); break;
                case '\f': b.append("\\f"); break;
                case '\n': b.append("\\n"); break;
                case '\r': b.append("\\r"); break;
                case '\t': b.append("\\t"); break;
                default:
                    if (c < 0x20) {
                        b.append("\\u00");
                        b.append(HEX_CHARS[(c >> 4) & 0xF]);
                        b.append(HEX_CHARS[c & 0xF]);
                    } else {
                        b.append(c);
                    }
            }
        }
    }

    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

    private static boolean isWhitespace(char c) {
        return c == ' ' || c == '\n' || c == '\r' || c == '\t';
    }

    private static Duration sanitizePositive(Duration v, Duration def) {
        if (v == null || v.isNegative() || v.isZero()) return def;
        return v;
    }

    private static String normalizeRequired(String s, String name) {
        Objects.requireNonNull(name, "name");
        if (s == null) throw new IllegalArgumentException(name + " is required");
        final String t = s.trim();
        if (t.isEmpty()) throw new IllegalArgumentException(name + " is required");
        return t;
    }
}
