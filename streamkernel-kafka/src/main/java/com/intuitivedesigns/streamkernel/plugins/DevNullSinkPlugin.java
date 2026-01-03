/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.plugins;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.OutputSink;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.spi.PluginKind;
import com.intuitivedesigns.streamkernel.spi.SinkPlugin;

import java.util.Objects;

public final class DevNullSinkPlugin implements SinkPlugin {

    public static final String ID = "DEVNULL";

    @Override
    public String id() {
        return ID;
    }

    @Override
    public PluginKind kind() {
        return PluginKind.SINK;
    }

    @Override
    public OutputSink<?> create(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");
        return new DevNullSink<>();
    }

    private static final class DevNullSink<T> implements OutputSink<T> {
        @Override
        public void write(PipelinePayload<T> payload) {
            // If your OutputSink interface is OutputSink<T> { void write(PipelinePayload<T> p); }
            // this method signature may differ in your project. Adjust accordingly.
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
