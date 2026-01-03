/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.security;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.spi.PluginKind;
import com.intuitivedesigns.streamkernel.spi.SecurityPlugin;
import com.intuitivedesigns.streamkernel.spi.SecurityProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PermitAllSecurityPlugin implements SecurityPlugin {

    public static final String ID = "PERMIT_ALL";
    private static final Logger log = LoggerFactory.getLogger(PermitAllSecurityPlugin.class);

    @Override
    public String id() {
        return ID;
    }

    @Override
    public PluginKind kind() {
        return PluginKind.SECURITY;
    }

    @Override
    public SecurityProvider create(PipelineConfig config, MetricsRuntime metrics) {
        log.warn("SECURITY WARNING: Using PERMIT_ALL policy. All actions are allowed.");
        return new PermitAllProvider();
    }

    // -- Implementation --
    private static final class PermitAllProvider implements SecurityProvider {

        @Override
        public boolean isAllowed(String principal, String resource, String action) {
            // In the future OPA integration, this boolean comes from the Rego evaluation
            return true; 
        }

        @Override
        public void close() {
            // No-op
        }
    }
}
