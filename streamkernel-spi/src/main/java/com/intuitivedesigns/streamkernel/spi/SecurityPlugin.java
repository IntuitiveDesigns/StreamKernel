/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.spi;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;

/**
 * SPI Definition for Security Providers.
 *
 * Example IDs: "OPA", "KEYCLOAK", "ALLOW_ALL"
 */
public interface SecurityPlugin extends PipelinePlugin<SecurityProvider> {

    String id();

    @Override
    default PluginKind kind() {
        return PluginKind.SECURITY;
    }

    @Override
    SecurityProvider create(PipelineConfig config, MetricsRuntime metrics) throws Exception;
}