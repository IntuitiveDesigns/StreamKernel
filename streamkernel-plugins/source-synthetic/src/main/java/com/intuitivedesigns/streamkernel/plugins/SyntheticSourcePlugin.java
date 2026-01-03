/*
 * Copyright 2026 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.plugins;

import com.intuitivedesigns.streamkernel.bench.SyntheticSource;
import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.SourceConnector;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.spi.PluginKind;
import com.intuitivedesigns.streamkernel.spi.SourcePlugin;

import java.util.Objects;

public final class SyntheticSourcePlugin implements SourcePlugin {

    public static final String ID = "SYNTHETIC";

    // Config Keys
    private static final String CFG_PAYLOAD_SIZE = "source.synthetic.payload.size";
    private static final String CFG_BUFFER_SIZE  = "source.synthetic.buffer.size";
    private static final String CFG_HIGH_ENTROPY = "source.synthetic.high.entropy";
    private static final String CFG_UNSAFE_REUSE = "source.unsafe.reuse.batch";

    // Defaults
    private static final int DEFAULT_PAYLOAD_SIZE = 1024;
    private static final int DEFAULT_BUFFER_SIZE  = 262_144; // 2^18
    private static final boolean DEFAULT_HIGH_ENTROPY = false;
    private static final boolean DEFAULT_UNSAFE_REUSE = false;

    @Override
    public String id() {
        return ID;
    }

    @Override
    public PluginKind kind() {
        return PluginKind.SOURCE;
    }

    @Override
    public SourceConnector<?> create(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");

        int payloadSize = clamp(
                config.getInt(CFG_PAYLOAD_SIZE, DEFAULT_PAYLOAD_SIZE),
                1, 
                Integer.MAX_VALUE
        );

        int bufferSize = clamp(
                config.getInt(CFG_BUFFER_SIZE, DEFAULT_BUFFER_SIZE),
                1024,
                1 << 30
        );

        boolean highEntropy = config.getBoolean(CFG_HIGH_ENTROPY, DEFAULT_HIGH_ENTROPY);
        boolean unsafeReuse = config.getBoolean(CFG_UNSAFE_REUSE, DEFAULT_UNSAFE_REUSE);

        return new SyntheticSource(payloadSize, bufferSize, highEntropy, unsafeReuse);
    }

    private static int clamp(int v, int min, int max) {
        return Math.max(min, Math.min(max, v));
    }
}
