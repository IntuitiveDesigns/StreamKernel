/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.plugins.transform;

import com.intuitivedesigns.streamkernel.avro.CustomerEvent;
import com.intuitivedesigns.streamkernel.avro.EnrichedTicket;
import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.core.Transformer;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class AiEnrichmentTransformer implements Transformer<CustomerEvent, EnrichedTicket> {

    private static final Logger log = LoggerFactory.getLogger(AiEnrichmentTransformer.class);

    private static final String CT = "Content-Type";
    private static final String APP_JSON = "application/json";
    private static final String AUTH = "Authorization";
    private static final String BEARER = "Bearer ";

    private static final Pattern EMBEDDING_PATTERN =
            Pattern.compile("\"embedding\"\\s*:\\s*\\[([0-9eE+\\-.,\\s]+)\\]");

    private static final int DEFAULT_CONNECT_TIMEOUT_MS = 2_000;
    private static final int DEFAULT_REQUEST_TIMEOUT_MS = 2_000;
    private static final int DEFAULT_MOCK_DIM = 5;

    private static final String KEY_MOCK = "ai.enrichment.mock";
    private static final String KEY_URL = "ai.enrichment.url";
    private static final String KEY_API_KEY = "ai.enrichment.key";
    private static final String KEY_TIMEOUT_MS = "ai.enrichment.timeout.ms";
    private static final String KEY_CONNECT_TIMEOUT_MS = "ai.enrichment.connect.timeout.ms";
    private static final String KEY_MODEL = "ai.enrichment.model";
    private static final String KEY_MOCK_DIM = "ai.enrichment.mock.dim";

    private static final String DEFAULT_URL = "http://localhost:11434/api/embeddings";
    private static final String DEFAULT_MODEL = "nomic-embed-text";

    private static final String JSON_PREFIX_HEAD = "{\"model\":\"";
    private static final String JSON_MID = "\",\"prompt\":\"";
    private static final String JSON_SUFFIX = "\"}";

    private final boolean useMock;
    private final MetricsRuntime metrics;

    private final HttpClient httpClient;
    private final URI apiUri;
    private final String apiKey;
    private final boolean hasApiKey;
    private final Duration requestTimeout;

    private final String jsonPrefix; // {"model":"<model>","prompt":"
    private final int mockDim;

    public AiEnrichmentTransformer(PipelineConfig config, MetricsRuntime metrics) {
        this.metrics = metrics;

        final PipelineConfig cfg = config;
        this.useMock = (cfg == null) || cfg.getBoolean(KEY_MOCK, true);

        if (!useMock) {
            this.apiUri = URI.create(cfg.getString(KEY_URL, DEFAULT_URL));
            this.apiKey = normalize(cfg.getString(KEY_API_KEY, null));
            this.hasApiKey = (apiKey != null);

            final long timeoutMs = Math.max(100L, cfg.getLong(KEY_TIMEOUT_MS, DEFAULT_REQUEST_TIMEOUT_MS));
            final long connectTimeoutMs = Math.max(100L, cfg.getLong(KEY_CONNECT_TIMEOUT_MS, DEFAULT_CONNECT_TIMEOUT_MS));

            this.requestTimeout = Duration.ofMillis(timeoutMs);
            this.httpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofMillis(connectTimeoutMs))
                    .version(HttpClient.Version.HTTP_2)
                    .build();

            final String model = normalizeOrDefault(cfg.getString(KEY_MODEL, DEFAULT_MODEL), DEFAULT_MODEL);
            this.jsonPrefix = JSON_PREFIX_HEAD + model + JSON_MID;

            this.mockDim = 0;

            log.info("AI Transformer: REAL MODE (Target: {})", apiUri);
        } else {
            this.apiUri = null;
            this.apiKey = null;
            this.hasApiKey = false;
            this.httpClient = null;
            this.requestTimeout = null;
            this.jsonPrefix = null;

            final int dim = (cfg != null) ? (int) cfg.getLong(KEY_MOCK_DIM, DEFAULT_MOCK_DIM) : DEFAULT_MOCK_DIM;
            this.mockDim = Math.max(1, Math.min(16_384, dim));

            log.info("AI Transformer: MOCK MODE (dim={})", mockDim);
        }
    }

    @Override
    public PipelinePayload<EnrichedTicket> transform(PipelinePayload<CustomerEvent> input) {
        if (input == null) return null;

        final CustomerEvent raw = input.data();
        if (raw == null) return null;

        final long startNs = System.nanoTime();

        final String text = (raw.getName() != null) ? raw.getName() : "";
        final String sentiment = detectSentiment(text);

        final List<Float> vector;
        try {
            vector = useMock ? mockEmbedding(mockDim) : fetchEmbedding(text);
        } catch (Exception e) {
            if (metrics != null) metrics.counter("ai.enrichment.errors", 1.0);
            log.error("Embedding generation failed for ID: {}", raw.getCustomerId(), e);
            throw new RuntimeException("AI Enrichment Failed", e);
        }

        final EnrichedTicket enriched = EnrichedTicket.newBuilder()
                .setTicketId(raw.getCustomerId())
                .setDescription(text)
                .setSentiment(sentiment)
                .setEmbedding(vector)
                .build();

        if (metrics != null) {
            metrics.timer("ai.enrichment.latency", (System.nanoTime() - startNs) / 1_000_000L);
        }

        return input.withData(enriched);
    }

    private List<Float> fetchEmbedding(String text) throws Exception {
        final String jsonBody = buildJsonBody(text);

        HttpRequest.Builder b = HttpRequest.newBuilder()
                .uri(apiUri)
                .timeout(requestTimeout)
                .header(CT, APP_JSON)
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody, StandardCharsets.UTF_8));

        if (hasApiKey) {
            b.header(AUTH, BEARER + apiKey);
        }

        final HttpResponse<String> response =
                httpClient.send(b.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

        if (response.statusCode() != 200) {
            throw new RuntimeException("AI API HTTP " + response.statusCode());
        }

        return parseEmbedding(response.body());
    }

    private String buildJsonBody(String text) {
        final int n = (text == null) ? 0 : text.length();
        final StringBuilder sb = new StringBuilder(jsonPrefix.length() + JSON_SUFFIX.length() + n + 16);
        sb.append(jsonPrefix);
        escapeJson(sb, text);
        sb.append(JSON_SUFFIX);
        return sb.toString();
    }

    private static List<Float> mockEmbedding(int dim) {
        final ThreadLocalRandom rng = ThreadLocalRandom.current();
        final ArrayList<Float> v = new ArrayList<>(dim);
        for (int i = 0; i < dim; i++) v.add(rng.nextFloat());
        return v;
    }

    private static String detectSentiment(String text) {
        if (text == null || text.isEmpty()) return "NEUTRAL";

        final String lower = text.toLowerCase();

        if (lower.contains("fail") || lower.contains("broken") || lower.contains("error")) return "NEGATIVE";
        if (lower.contains("great") || lower.contains("thanks") || lower.contains("working")) return "POSITIVE";
        return "NEUTRAL";
    }

    private static List<Float> parseEmbedding(String json) {
        if (json == null || json.isEmpty()) return List.of();

        final Matcher m = EMBEDDING_PATTERN.matcher(json);
        if (!m.find()) return List.of();

        final String body = m.group(1);
        if (body == null || body.isEmpty()) return List.of();

        final ArrayList<Float> out = new ArrayList<>(64);

        final int len = body.length();
        int i = 0;

        while (i < len) {
            char c;
            while (i < len && ((c = body.charAt(i)) == ' ' || c == '\n' || c == '\r' || c == '\t' || c == ',')) i++;
            if (i >= len) break;

            final int start = i;
            while (i < len) {
                c = body.charAt(i);
                if (c == ',' || c == ' ' || c == '\n' || c == '\r' || c == '\t') break;
                i++;
            }

            if (i > start) {
                try {
                    out.add(Float.parseFloat(body.substring(start, i)));
                } catch (Exception ignored) {
                }
            }
        }

        return out.isEmpty() ? List.of() : out;
    }

    private static String normalize(String s) {
        if (s == null) return null;
        final String t = s.trim();
        return t.isEmpty() ? null : t;
    }

    private static String normalizeOrDefault(String s, String def) {
        final String n = normalize(s);
        return (n != null) ? n : def;
    }

    private static void escapeJson(StringBuilder b, String s) {
        if (s == null || s.isEmpty()) return;

        final int len = s.length();
        for (int i = 0; i < len; i++) {
            final char c = s.charAt(i);
            switch (c) {
                case '"':  b.append("\\\""); break;
                case '\\': b.append("\\\\"); break;
                case '\b': b.append("\\b"); break;
                case '\f': b.append("\\f"); break;
                case '\n':
                case '\r':
                case '\t':
                    b.append(' ');
                    break;
                default:
                    if (c < 0x20) b.append(' ');
                    else b.append(c);
            }
        }
    }
}
