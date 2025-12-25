/*
 * Copyright 2025 Steven Lopez
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
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
    private final Duration requestTimeout;

    public OpaAuthorizer(String opaUrl) {
        this(opaUrl, Duration.ofMillis(300), Duration.ofMillis(200));
    }

    public OpaAuthorizer(String opaUrl, Duration connectTimeout, Duration requestTimeout) {
        this.client = HttpClient.newBuilder()
                .connectTimeout(connectTimeout == null ? Duration.ofMillis(300) : connectTimeout)
                .build();

        this.opaUri = URI.create(opaUrl);
        this.requestTimeout = (requestTimeout == null) ? Duration.ofMillis(200) : requestTimeout;
    }

    public boolean isAllowed(String user, String action, String resource) {
        // Build JSON without allocations from String.format
        String body = buildBody(user, action, resource);

        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(opaUri)
                    .timeout(requestTimeout)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                log.debug("OPA non-2xx response. status={} body={}", response.statusCode(), response.body());
                return false; // fail-closed
            }

            return parseOpaResult(response.body());

        } catch (Exception e) {
            log.error("OPA check failed (fail-closed)", e);
            return false;
        }
    }

    private static String buildBody(String user, String action, String resource) {
        String u = escapeJson(user);
        String a = escapeJson(action);
        String r = escapeJson(resource);

        // {"input":{"user":"...","action":"...","resource":"..."}}
        StringBuilder sb = new StringBuilder(u.length() + a.length() + r.length() + 64);
        sb.append("{\"input\":{\"user\":\"").append(u)
                .append("\",\"action\":\"").append(a)
                .append("\",\"resource\":\"").append(r)
                .append("\"}}");
        return sb.toString();
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
