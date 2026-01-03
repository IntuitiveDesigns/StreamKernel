/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.spi;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;

/**
 * SPI Factory for creating Cache instances.
 * MUST extend PipelinePlugin<Cache<?, ?>> to match the PipelineFactory return type.
 */
public interface CachePlugin extends PipelinePlugin<Cache<?, ?>> {
    
    /**
     * Creates a new Cache instance.
     * The signature matches the generic definition in PipelinePlugin.
     */
    @Override
    Cache<?, ?> create(PipelineConfig config, MetricsRuntime metrics);
}
