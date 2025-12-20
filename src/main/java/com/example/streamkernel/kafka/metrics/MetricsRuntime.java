package com.example.streamkernel.kafka.metrics;

import io.micrometer.core.instrument.MeterRegistry;

public interface MetricsRuntime extends AutoCloseable {
    MeterRegistry registry();
    boolean enabled();
    String type();

    @Override
    void close();
}
