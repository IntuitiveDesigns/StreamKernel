/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */
package com.intuitivedesigns.streamkernel.spi;

public interface ServicePlugin {
    /**
     * @return The unique ID of this plugin implementation (e.g., 'KAFKA', 'NOOP').
     */
    String id();

    /**
     * @return The kind of plugin (SOURCE, SINK, etc.).
     */
    PluginKind kind();
}
