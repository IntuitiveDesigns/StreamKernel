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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * A high-performance 'Black Hole' sink for benchmarking.
 * Discards all data immediately with near-zero overhead.
 * <p>
 * ID: DEVNULL
 */
public final class DevNullSinkPlugin implements SinkPlugin {

    public static final String ID = "DEVNULL";
    private static final Logger log = LoggerFactory.getLogger(DevNullSinkPlugin.class);

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
        
        // Log at WARN level so operators are explicitly aware data is being lost
        log.warn("⚠ ACTIVATING DEVNULL SINK: All pipeline output will be discarded!");
        
        return new DevNullSink<>();
    }

    /**
     * Internal implementation of the No-Op sink.
     * parameterized to allow type-safe usage in the pipeline.
     */
    private static final class DevNullSink<T> implements OutputSink<T> {
        
        @Override
        public void write(PipelinePayload<T> payload) {
            // Intentional No-Op for maximum throughput benchmarking.
            // We do not count metrics here to avoid Atomic overhead; 
            // the Orchestrator handles throughput measurement.
        }

        @Override
        public void close() {
            log.info("DevNull sink closed.");
        }
    }
}
