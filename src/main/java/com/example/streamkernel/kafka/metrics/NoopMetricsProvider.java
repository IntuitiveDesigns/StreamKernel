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
