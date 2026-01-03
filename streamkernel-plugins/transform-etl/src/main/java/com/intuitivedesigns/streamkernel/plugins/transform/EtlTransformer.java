/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.plugins.transform;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.core.Transformer;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Standard ETL Transformer.
 * Responsibilities:
 * 1. PII Redaction (Email Masking)
 * 2. Header Enrichment (Provenance)
 * 3. Observability (Metrics)
 */
public final class EtlTransformer implements Transformer<String, String> {

    private static final Logger log = LoggerFactory.getLogger(EtlTransformer.class);

    // Keep regex simple; avoid pathological backtracking.
    private static final Pattern EMAIL_PATTERN =
            Pattern.compile("[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[A-Za-z]{2,}");
    private static final String MASK_REPLACEMENT = "***@***.com";

    private static final String HDR_PROCESSED_BY = "processed_by";
    private static final String HDR_PRIVACY_CHECK = "privacy_check";
    private static final String HDR_PROCESSING_NS = "processing_time_ns";

    private static final String PRIV_REDACTED = "REDACTED";
    private static final String PRIV_CLEAN = "CLEAN";

    private final MetricsRuntime metrics;
    private final boolean traceEnabled;

    public EtlTransformer(PipelineConfig config, MetricsRuntime metrics) {
        this.metrics = metrics;
        this.traceEnabled = config != null && config.getBoolean("etl.trace.enabled", false);
    }

    @Override
    public PipelinePayload<String> transform(PipelinePayload<String> input) {
        if (input == null) return null;

        final String raw = input.data();
        if (raw == null) return null;

        final long startNs = System.nanoTime();
        boolean piiFound = false;

        String out = raw;

        // Fast gate before regex work
        if (raw.indexOf('@') >= 0) {
            final Matcher m = EMAIL_PATTERN.matcher(raw);
            if (m.find()) {
                piiFound = true;
                // Only allocate when needed
                out = m.replaceAll(MASK_REPLACEMENT);
            }
        }

        // Headers: single copy, sized to avoid rehash
        final Map<String, String> in = input.headers();
        final int base = (in == null) ? 0 : in.size();
        final HashMap<String, String> headers = (base == 0)
                ? new HashMap<>(8)
                : new HashMap<>((int) ((base + 3) / 0.75f) + 1);

        if (base != 0) headers.putAll(in);

        headers.put(HDR_PROCESSED_BY, "EtlTransformer");
        headers.put(HDR_PRIVACY_CHECK, piiFound ? PRIV_REDACTED : PRIV_CLEAN);

        final long elapsedNs = System.nanoTime() - startNs;
        headers.put(HDR_PROCESSING_NS, Long.toString(elapsedNs));

        if (metrics != null) {
            metrics.counter("etl.records.processed", 1.0);
            if (piiFound) metrics.counter("etl.pii.masked", 1.0);
            // Optional: keep timer if your MetricsRuntime supports it; otherwise omit.
            // metrics.timer("etl.transform.latency.ms", elapsedNs / 1_000_000L);
        }

        if (traceEnabled && log.isDebugEnabled()) {
            log.debug("[ETL] ID={} PII={} Latency={}ns", input.id(), piiFound, elapsedNs);
        }

        return new PipelinePayload<>(
                input.id(),
                out,
                input.timestamp(),
                headers
        );
    }
}
