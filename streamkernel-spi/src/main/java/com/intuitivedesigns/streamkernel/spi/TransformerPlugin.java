/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.spi;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.Transformer;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;

/**
 * SPI Definition for Pipeline Transformers.
 *
 * <p><b>Generics Note:</b> We use {@code Transformer<?, ?>} (wildcards) here because
 * transformers often change data types (e.g., JSON String -> POJO).
 * The Pipeline Orchestrator is responsible for ensuring type compatibility
 * at runtime.</p>
 */
public interface TransformerPlugin extends PipelinePlugin<Transformer<?, ?>> {

    String id(); // e.g. "UPPERCASE", "PII_REDACT", "AI_ENRICH"

    @Override
    default PluginKind kind() {
        return PluginKind.TRANSFORMER;
    }

    @Override
    Transformer<?, ?> create(PipelineConfig config, MetricsRuntime metrics) throws Exception;
}