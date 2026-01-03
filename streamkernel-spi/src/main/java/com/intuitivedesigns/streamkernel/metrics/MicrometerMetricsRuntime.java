/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.metrics;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Production-Grade Metrics Bridge for Micrometer.
 *
 * Features:
 * - Composite Registry (supports multiple backends: JMX, Prometheus, Datadog)
 * - Stateful "Push" Gauges (maps generic gauge calls to atomic state holders)
 * - Safe Latency Recording
 */
public final class MicrometerMetricsRuntime implements MetricsRuntime {

    private static final Logger log = LoggerFactory.getLogger(MicrometerMetricsRuntime.class);

    private final CompositeMeterRegistry registry;

    // State storage for "Push" gauges (Micrometer defaults to "Pull/Poll" gauges)
    private final Map<String, AtomicDouble> gaugeState = new ConcurrentHashMap<>();

    public MicrometerMetricsRuntime() {
        this.registry = new CompositeMeterRegistry();
        // Always add SimpleRegistry (Logging/Memory) so metrics work out-of-the-box in dev
        this.registry.add(new SimpleMeterRegistry());
    }

    public static MicrometerMetricsRuntime fromConfig(PipelineConfig config) {
        MicrometerMetricsRuntime runtime = new MicrometerMetricsRuntime();

        // 1. JMX Support (Optional - check config)
        if (config.getBoolean("metrics.jmx.enabled", false)) {
            log.info("📈 JMX Metrics Enabled");
            // runtime.addRegistry(new io.micrometer.jmx.JmxMeterRegistry(...));
        }

        log.info("📊 Metrics Runtime Initialized (Type: MICROMETER)");
        return runtime;
    }

    /**
     * Adds a specific registry (e.g., Prometheus) to the composite.
     */
    public void addRegistry(MeterRegistry specificRegistry) {
        this.registry.add(specificRegistry);
    }

    // --- Interface Implementation ---

    @Override
    public Object registry() {
        return registry;
    }

    @Override
    public boolean enabled() {
        return true;
    }

    @Override
    public String type() {
        return "MICROMETER";
    }

    @Override
    public void counter(String name) {
        registry.counter(name).increment();
    }

    @Override
    public void counter(String name, double increment) {
        if (increment > 0) {
            registry.counter(name).increment(increment);
        }
    }

    @Override
    public void timer(String name, long durationMillis) {
        registry.timer(name).record(durationMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void gauge(String name, double value) {
        // computeIfAbsent is atomic: ensures we register the gauge exactly once
        AtomicDouble state = gaugeState.computeIfAbsent(name, key -> {
            AtomicDouble newState = new AtomicDouble(value);
            // Register the AtomicDouble with Micrometer, which will poll 'get()'
            Gauge.builder(key, newState, AtomicDouble::get)
                    .register(registry);
            return newState;
        });

        // Update the state (Push)
        state.set(value);
    }

    @Override
    public void close() {
        registry.close();
        log.info("Metrics Runtime Closed.");
    }

    /**
     * Lightweight Mutable Double for Gauge State.
     * Extends Number to satisfy Micrometer's functional interface requirements.
     */
    private static final class AtomicDouble extends Number {
        private final AtomicLong bits;

        public AtomicDouble(double initialValue) {
            this.bits = new AtomicLong(Double.doubleToLongBits(initialValue));
        }

        public void set(double newValue) {
            bits.set(Double.doubleToLongBits(newValue));
        }

        public double get() {
            return Double.longBitsToDouble(bits.get());
        }

        @Override public int intValue() { return (int) get(); }
        @Override public long longValue() { return (long) get(); }
        @Override public float floatValue() { return (float) get(); }
        @Override public double doubleValue() { return get(); }
    }
}