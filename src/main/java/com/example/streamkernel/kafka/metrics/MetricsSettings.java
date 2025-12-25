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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.example.streamkernel.kafka.metrics;

import com.example.streamkernel.kafka.config.PipelineConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class MetricsSettings {

    public final boolean enabled;
    public final String type; // PROMETHEUS | DATADOG | NONE

    // Prometheus
    public final int prometheusPort;

    // Datadog
    public final Duration step;
    public final String datadogApiKey;
    public final String datadogUri;
    public final String datadogHostTag;

    // Common tags
    public final String serviceName;
    public final String env;
    public final String pipelineName;
    public final String pipelineVersion;
    public final Map<String, String> extraTags; // metrics.tags.*

    private MetricsSettings(
            boolean enabled,
            String type,
            int prometheusPort,
            Duration step,
            String datadogApiKey,
            String datadogUri,
            String datadogHostTag,
            String serviceName,
            String env,
            String pipelineName,
            String pipelineVersion,
            Map<String, String> extraTags
    ) {
        this.enabled = enabled;
        this.type = type;
        this.prometheusPort = prometheusPort;
        this.step = step;
        this.datadogApiKey = datadogApiKey;
        this.datadogUri = datadogUri;
        this.datadogHostTag = datadogHostTag;

        this.serviceName = serviceName;
        this.env = env;
        this.pipelineName = pipelineName;
        this.pipelineVersion = pipelineVersion;
        this.extraTags = extraTags;
    }

    public static MetricsSettings from(PipelineConfig config) {
        boolean enabled = Boolean.parseBoolean(
                System.getProperty("streamkernel.metrics.enabled",
                        config.getProperty("metrics.enabled", "true"))
        );

        String type = config.getProperty("metrics.type", "PROMETHEUS").trim().toUpperCase();

        int port = Integer.parseInt(config.getProperty("metrics.prometheus.port", "8080"));

        Duration step = Duration.ofSeconds(Integer.parseInt(config.getProperty("metrics.step.seconds", "10")));

        // Datadog
        String apiKey = firstNonBlank(System.getenv("DD_API_KEY"), config.getProperty("metrics.datadog.apiKey", ""));
        // Default to standard US site; allow override for EU, Gov, etc.
        String uri = firstNonBlank(config.getProperty("metrics.datadog.uri", ""), System.getenv("DD_SITE"));
        // If DD_SITE is provided (e.g., datadoghq.eu), build an api URL; else fallback.
        String ddUri = normalizeDatadogUri(uri);

        String hostTag = config.getProperty("metrics.datadog.hostTag", "host");

        // Common tags (sane defaults)
        String pipelineName = config.getProperty("pipeline.name", "StreamKernel");
        String pipelineVersion = config.getProperty("pipeline.version", "dev");
        String serviceName = config.getProperty("metrics.service", "streamkernel");
        String env = firstNonBlank(System.getenv("DD_ENV"), config.getProperty("metrics.env", "local"));

        Map<String, String> extraTags = readPrefixedTags(config, "metrics.tags.");

        return new MetricsSettings(
                enabled,
                type,
                port,
                step,
                apiKey,
                ddUri,
                hostTag,
                serviceName,
                env,
                pipelineName,
                pipelineVersion,
                extraTags
        );
    }

    private static Map<String, String> readPrefixedTags(PipelineConfig config, String prefix) {
        try {
            Map<String, String> tags = new LinkedHashMap<>();
            for (String k : config.keys()) {
                if (k != null && k.startsWith(prefix)) {
                    String tagKey = k.substring(prefix.length()).trim();
                    String tagVal = config.getProperty(k, "").trim();
                    if (!tagKey.isEmpty() && !tagVal.isEmpty()) {
                        tags.put(tagKey, tagVal);
                    }
                }
            }
            return Collections.unmodifiableMap(tags);
        } catch (Exception ignored) {
            return Collections.emptyMap();
        }
    }

    private static String normalizeDatadogUri(String uriOrSite) {
        if (uriOrSite == null || uriOrSite.isBlank()) {
            return "https://api.datadoghq.com";
        }
        String s = uriOrSite.trim();

        // If user provides full URL, use it.
        if (s.startsWith("http://") || s.startsWith("https://")) {
            return s;
        }

        // If user provides DD_SITE-like value, build api URL.
        // e.g. "datadoghq.eu" -> "https://api.datadoghq.eu"
        return "https://api." + s;
    }

    private static String firstNonBlank(String a, String b) {
        if (a != null && !a.isBlank()) return a;
        if (b != null && !b.isBlank()) return b;
        return null;
    }
}
