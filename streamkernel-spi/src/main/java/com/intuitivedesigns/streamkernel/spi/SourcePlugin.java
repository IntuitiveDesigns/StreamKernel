/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.spi;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.SourceConnector;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;

/**
 * SPI Definition for Pipeline Sources.
 *
 * <p><b>Generics Note:</b> {@code SourceConnector<?>} allows sources to produce
 * any type of data (String, AvroRecord, byte[]). The Orchestrator handles
 * type bridging at runtime.</p>
 */
public interface SourcePlugin extends PipelinePlugin<SourceConnector<?>> {

    String id(); // e.g. "SYNTHETIC", "KAFKA", "REST"

    @Override
    default PluginKind kind() {
        return PluginKind.SOURCE;
    }

    @Override
    SourceConnector<?> create(PipelineConfig config, MetricsRuntime metrics) throws Exception;
}