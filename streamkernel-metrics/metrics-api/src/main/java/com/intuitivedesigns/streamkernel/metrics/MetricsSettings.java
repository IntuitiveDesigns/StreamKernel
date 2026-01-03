/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.metrics;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable configuration container for Metrics Runtime.
 */
public final class MetricsSettings {

    // ---- Config keys ----
    private static final String KEY_PROVIDER = "metrics.provider";
    private static final String KEY_STEP_SECONDS = "metrics.step.seconds";
    private static final String KEY_TAG_PREFIX = "metrics.tag.";

    // Prometheus
    private static final String KEY_PROM_PORT = "metrics.prometheus.port";

    // Datadog
    private static final String KEY_DD_API_KEY = "metrics.datadog.apiKey";
    private static final String KEY_DD_URI = "metrics.datadog.uri";
    private static final String KEY_DD_HOST_TAG = "metrics.datadog.hostTag";

    // Env vars
    private static final String ENV_DD_API_KEY = "DD_API_KEY";

    // ---- Defaults ----
    private static final String DEFAULT_PROVIDER = "NONE";
    private static final int DEFAULT_STEP_SECONDS = 10;
    private static final int DEFAULT_PROM_PORT = 9090;
    private static final String DEFAULT_DD_URI = "https://api.datadoghq.com";

    // ---- Public Immutable Fields ----
    public final String providerId;
    public final Map<String, String> commonTags;
    public final Duration step;

    public final int prometheusPort;

    public final String datadogApiKey;
    public final String datadogUri;
    public final String datadogHostTag;

    private MetricsSettings(String providerId,
                            Map<String, String> commonTags,
                            Duration step,
                            int prometheusPort,
                            String datadogApiKey,
                            String datadogUri,
                            String datadogHostTag) {
        this.providerId = providerId;
        this.commonTags = commonTags;
        this.step = step;
        this.prometheusPort = prometheusPort;
        this.datadogApiKey = datadogApiKey;
        this.datadogUri = datadogUri;
        this.datadogHostTag = datadogHostTag;
    }

    public static MetricsSettings from(PipelineConfig config) {
        Objects.requireNonNull(config, "config");

        final String provider = normalizeUpper(config.getString(KEY_PROVIDER, DEFAULT_PROVIDER));

        final int stepSec = clampInt(config.getInt(KEY_STEP_SECONDS, DEFAULT_STEP_SECONDS), 1, 3_600);
        final Duration step = Duration.ofSeconds(stepSec);

        // --- Tag Parsing ---
        final Map<String, String> tags = new HashMap<>();
        for (Map.Entry<String, Object> entry : safeMap(config).entrySet()) {
            final String k = entry.getKey();
            if (k == null || !k.startsWith(KEY_TAG_PREFIX)) continue;

            final String tagKey = k.substring(KEY_TAG_PREFIX.length()).trim();
            if (tagKey.isEmpty()) continue;

            final Object valObj = entry.getValue();
            if (valObj == null) continue;

            final String valStr = String.valueOf(valObj).trim();
            if (valStr.isEmpty()) continue;

            tags.put(tagKey, valStr);
        }

        // --- Provider Specifics ---
        final int promPort = clampInt(config.getInt(KEY_PROM_PORT, DEFAULT_PROM_PORT), 1, 65_535);

        // Hierarchy: Env Var > Config File
        final String ddKey = firstNonBlank(
                env(ENV_DD_API_KEY),
                config.getString(KEY_DD_API_KEY, null)
        );

        final String ddUri = firstNonBlank(
                config.getString(KEY_DD_URI, DEFAULT_DD_URI),
                DEFAULT_DD_URI
        );

        final String ddHostTag = normalize(config.getString(KEY_DD_HOST_TAG, null));

        return new MetricsSettings(
                provider,
                Collections.unmodifiableMap(tags),
                step,
                promPort,
                ddKey,
                ddUri,
                ddHostTag
        );
    }

    @Override
    public String toString() {
        return "MetricsSettings{" +
                "providerId='" + providerId + '\'' +
                ", commonTags=" + commonTags +
                ", step=" + step +
                ", prometheusPort=" + prometheusPort +
                ", datadogUri='" + datadogUri + '\'' +
                ", datadogHostTag='" + datadogHostTag + '\'' +
                ", datadogApiKey=" + mask(datadogApiKey) +
                '}';
    }

    // --- Helpers ---

    private static Map<String, Object> safeMap(PipelineConfig config) {
        try {
            Map<String, Object> m = config.asMap();
            return (m != null) ? m : Map.of();
        } catch (Exception ignored) {
            return Map.of();
        }
    }

    private static String env(String k) {
        try {
            return System.getenv(k);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static String firstNonBlank(String a, String b) {
        String s = normalize(a);
        return (s != null) ? s : normalize(b);
    }

    private static String normalize(String s) {
        if (s == null) return null;
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }

    private static String normalizeUpper(String s) {
        String n = normalize(s);
        return (n != null) ? n.toUpperCase(Locale.ROOT) : null;
    }

    private static int clampInt(int v, int min, int max) {
        return Math.max(min, Math.min(max, v));
    }

    private static String mask(String secret) {
        if (secret == null || secret.isEmpty()) return "****";
        if (secret.length() <= 4) return "****";
        return "****" + secret.substring(secret.length() - 4);
    }
}
