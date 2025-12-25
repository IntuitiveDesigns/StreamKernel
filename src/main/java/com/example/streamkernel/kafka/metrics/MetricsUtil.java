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
import io.micrometer.core.instrument.Tag;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

final class MetricsUtil {

    private MetricsUtil() {}

    static void applyCommonTags(MeterRegistry reg, MetricsSettings s) {
        if (reg == null || s == null) return;

        List<Tag> tags = new ArrayList<>(8);

        // Keep these stable/low-cardinality
        tags.add(Tag.of("service", safe(s.serviceName)));
        tags.add(Tag.of("env", safe(s.env)));
        tags.add(Tag.of("pipeline", safe(s.pipelineName)));
        tags.add(Tag.of("version", safe(s.pipelineVersion)));

        // Optional additional tags
        if (s.extraTags != null) {
            for (Map.Entry<String, String> e : s.extraTags.entrySet()) {
                if (e.getKey() != null && !e.getKey().isBlank() && e.getValue() != null && !e.getValue().isBlank()) {
                    tags.add(Tag.of(e.getKey(), e.getValue()));
                }
            }
        }

        reg.config().commonTags(tags);
    }

    private static String safe(String v) {
        return (v == null || v.isBlank()) ? "unknown" : v;
    }
}
