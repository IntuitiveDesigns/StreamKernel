/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class MetricsUtil {

    private MetricsUtil() {}

    /**
     * Apply common tags from settings to a Micrometer registry.
     */
    public static void applyCommonTags(MeterRegistry registry, MetricsSettings settings) {
        if (registry == null || settings == null) return;

        final Tags tags = toTags(settings.commonTags);

        // Tags implements Iterable, so passing an empty set is a safe no-op.
        registry.config().commonTags(tags);
    }

    /**
     * Convert a raw Map into Micrometer {@link Tags}.
     * Handles nulls, trimming, and empty strings defensively.
     */
    public static Tags toTags(Map<String, String> input) {
        if (input == null || input.isEmpty()) return Tags.empty();

        final List<Tag> out = new ArrayList<>(input.size());

        for (Map.Entry<String, String> e : input.entrySet()) {
            final String k = safe(e.getKey());
            final String v = safe(e.getValue());

            // Micrometer will throw NPE if key/value is null, so we skip invalid entries
            if (k != null && v != null) {
                out.add(Tag.of(k, v));
            }
        }

        return out.isEmpty() ? Tags.empty() : Tags.of(out);
    }

    private static String safe(String s) {
        if (s == null) return null;
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }
}