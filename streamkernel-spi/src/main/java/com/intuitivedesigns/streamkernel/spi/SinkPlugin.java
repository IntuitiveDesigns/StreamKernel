/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.spi;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.OutputSink;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;

/**
 * SPI Definition for Pipeline Sinks (Destinations).
 *
 * <p><b>Generics Note:</b> {@code OutputSink<?>} allows sinks to accept
 * specific data types. The Orchestrator handles the safety check to ensure
 * the last Transformer outputs a type that the Sink accepts.</p>
 */
public interface SinkPlugin extends PipelinePlugin<OutputSink<?>> {

    String id(); // e.g. "KAFKA", "DEVNULL", "POSTGRES", "DLQ_LOG"

    @Override
    default PluginKind kind() {
        return PluginKind.SINK;
    }

    @Override
    OutputSink<?> create(PipelineConfig config, MetricsRuntime metrics) throws Exception;
}