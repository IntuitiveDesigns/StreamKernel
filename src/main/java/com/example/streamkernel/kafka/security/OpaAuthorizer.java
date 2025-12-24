package com.example.streamkernel.kafka.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public final class OpaAuthorizer {

    private static final Logger log = LoggerFactory.getLogger(OpaAuthorizer.class);

    private final HttpClient client;
    private final URI opaUri;
    private final Duration timeout;

    public OpaAuthorizer(String opaUrl) {
        this(opaUrl, Duration.ofMillis(500));
    }

    public OpaAuthorizer(String opaUrl, Duration timeout) {
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(300))
                .build();
        this.opaUri = URI.create(opaUrl);
        this.timeout = (timeout == null) ? Duration.ofMillis(500) : timeout;
    }

    public boolean isAllowed(String user, String action, String resource) {
        String body = "{\"input\":{\"user\":\"" + escapeJson(user) +
                "\",\"action\":\"" + escapeJson(action) +
                "\",\"resource\":\"" + escapeJson(resource) + "\"}}";

        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(opaUri)
                    .timeout(timeout)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                // Fail closed
                log.debug("OPA non-2xx response. status={} body={}", response.statusCode(), response.body());
                return false;
            }

            return parseOpaResult(response.body());

        } catch (Exception e) {
            log.error("OPA check failed (fail-closed)", e);
            return false;
        }
    }

    static boolean parseOpaResult(String body) {
        if (body == null) return false;

        int idx = body.indexOf("\"result\"");
        if (idx < 0) return false;

        int colon = body.indexOf(':', idx);
        if (colon < 0) return false;

        int i = colon + 1;
        while (i < body.length() && Character.isWhitespace(body.charAt(i))) i++;

        if (body.regionMatches(true, i, "true", 0, 4)) return true;
        if (body.regionMatches(true, i, "false", 0, 5)) return false;

        // Fallback for {"result":{"allow":true}}
        int allowIdx = body.indexOf("\"allow\"", i);
        if (allowIdx >= 0) {
            int allowColon = body.indexOf(':', allowIdx);
            if (allowColon > 0) {
                int j = allowColon + 1;
                while (j < body.length() && Character.isWhitespace(body.charAt(j))) j++;
                if (body.regionMatches(true, j, "true", 0, 4)) return true;
            }
        }

        return false;
    }

    static String escapeJson(String s) {
        if (s == null) return "";
        StringBuilder b = new StringBuilder(s.length() + 16);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': b.append("\\\""); break;
                case '\\': b.append("\\\\"); break;
                case '\b': b.append("\\b"); break;
                case '\f': b.append("\\f"); break;
                case '\n': b.append("\\n"); break;
                case '\r': b.append("\\r"); break;
                case '\t': b.append("\\t"); break;
                default:
                    if (c < 0x20) b.append(String.format("\\u%04x", (int) c));
                    else b.append(c);
            }
        }
        return b.toString();
    }
}
