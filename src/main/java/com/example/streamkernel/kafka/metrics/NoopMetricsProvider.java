package com.example.streamkernel.kafka.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.noop.NoopMeterRegistry;

public final class NoopMetricsProvider implements MetricsProvider {
    @Override public String type() { return "NONE"; }

    @Override
    public MetricsRuntime create(MetricsSettings s) {
        MeterRegistry reg = new NoopMeterRegistry();
        return new MetricsRuntime() {
            @Override public MeterRegistry registry() { return reg; }
            @Override public boolean enabled() { return false; }
            @Override public String type() { return "NONE"; }
            @Override public void close() { /* noop */ }
        };
    }
}
