package com.example.streamkernel.kafka.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

public final class NoopMetricsProvider implements MetricsProvider {
    @Override public String type() { return "NONE"; }

    @Override
    public MetricsRuntime create(MetricsSettings s) {
        SimpleMeterRegistry reg = new SimpleMeterRegistry();
        return new MetricsRuntime() {
            @Override public MeterRegistry registry() { return reg; }
            @Override public boolean enabled() { return false; }
            @Override public String type() { return "NONE"; }
            @Override public void close() { reg.close(); }
        };
    }
}
