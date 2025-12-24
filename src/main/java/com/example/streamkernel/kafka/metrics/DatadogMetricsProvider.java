package com.example.streamkernel.kafka.metrics;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.datadog.DatadogConfig;
import io.micrometer.datadog.DatadogMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DatadogMetricsProvider implements MetricsProvider {
    private static final Logger log = LoggerFactory.getLogger(DatadogMetricsProvider.class);

    @Override public String type() { return "DATADOG"; }

    @Override
    public MetricsRuntime create(MetricsSettings s) {
        // Hard failover to NONE (no SimpleMeterRegistry)
        if (s.datadogApiKey == null || s.datadogApiKey.isBlank()) {
            log.warn("Datadog selected but no API key found (DD_API_KEY or metrics.datadog.apiKey). Falling back to NONE.");
            return new NoopMetricsProvider().create(s);
        }

        DatadogConfig cfg = new DatadogConfig() {
            @Override public String get(String k) { return null; }
            @Override public String apiKey() { return s.datadogApiKey; }
            @Override public String uri() { return s.datadogUri; }
            @Override public java.time.Duration step() { return s.step; }
            @Override public String hostTag() { return s.datadogHostTag; }
        };

        DatadogMeterRegistry reg = new DatadogMeterRegistry(cfg, Clock.SYSTEM);

        // Apply consistent tags across the entire app
        MetricsUtil.applyCommonTags(reg, s);

        return new MetricsRuntime() {
            @Override public MeterRegistry registry() { return reg; }
            @Override public boolean enabled() { return true; }
            @Override public String type() { return "DATADOG"; }
            @Override public void close() { reg.close(); }
        };
    }
}
