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
            @Override public void close() { /* noop */ }
        };
    }
}
