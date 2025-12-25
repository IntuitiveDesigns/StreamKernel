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
package com.example.streamkernel.kafka.metrics;

import com.example.streamkernel.kafka.config.PipelineConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public final class MetricsFactory {
    private static final Logger log = LoggerFactory.getLogger(MetricsFactory.class);

    private static final Map<String, MetricsProvider> PROVIDERS = Map.of(
            "PROMETHEUS", new PrometheusMetricsProvider(),
            "DATADOG", new DatadogMetricsProvider(),
            "NONE", new NoopMetricsProvider()
    );

    private MetricsFactory() {}

    public static MetricsRuntime init(PipelineConfig config) {
        MetricsSettings s = MetricsSettings.from(config);

        if (!s.enabled || "NONE".equals(s.type)) {
            return PROVIDERS.get("NONE").create(s);
        }

        MetricsProvider provider = PROVIDERS.getOrDefault(s.type, PROVIDERS.get("NONE"));
        if (provider.type().equals("NONE") && !"NONE".equals(s.type)) {
            log.warn("📊 Unknown metrics.type='{}'. Falling back to NONE.", s.type);
        }

        try {
            MetricsRuntime rt = provider.create(s);
            log.info("📊 Metrics initialized: enabled={}, type={}", rt.enabled(), rt.type());
            return rt;
        } catch (Exception e) {
            log.error("📊 Failed to initialize metrics provider '{}'. Falling back to NONE.", s.type, e);
            return PROVIDERS.get("NONE").create(s);
        }
    }
}
