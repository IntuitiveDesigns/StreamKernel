/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.spi;

/**
 * The central registry for all loaded plugins.
 * Holds typed registries for Sources, Sinks, Transformers, Caches, etc.
 */
public final class PluginCatalog {

    private final ServicePluginRegistry<SourcePlugin> sources;
    private final ServicePluginRegistry<SinkPlugin> sinks;
    private final ServicePluginRegistry<TransformerPlugin> transformers;
    private final ServicePluginRegistry<CachePlugin> caches;
    private final ServicePluginRegistry<DlqSerializerPlugin> dlqSerializers;
    private final ServicePluginRegistry<SecurityPlugin> security; // Added Missing Registry

    public PluginCatalog(ClassLoader cl) {
        // Initialize all registries using the provided ClassLoader
        this.sources = new ServicePluginRegistry<>(SourcePlugin.class, cl);
        this.sinks = new ServicePluginRegistry<>(SinkPlugin.class, cl);
        this.transformers = new ServicePluginRegistry<>(TransformerPlugin.class, cl);
        this.caches = new ServicePluginRegistry<>(CachePlugin.class, cl);
        this.dlqSerializers = new ServicePluginRegistry<>(DlqSerializerPlugin.class, cl);
        this.security = new ServicePluginRegistry<>(SecurityPlugin.class, cl);
    }

    // --- Accessors (Required by PipelineFactory) ---

    public ServicePluginRegistry<SourcePlugin> sources() {
        return sources;
    }

    public ServicePluginRegistry<SinkPlugin> sinks() {
        return sinks;
    }

    public ServicePluginRegistry<TransformerPlugin> transformers() {
        return transformers;
    }

    public ServicePluginRegistry<CachePlugin> caches() {
        return caches;
    }

    public ServicePluginRegistry<DlqSerializerPlugin> dlqSerializers() {
        return dlqSerializers;
    }

    public ServicePluginRegistry<SecurityPlugin> security() {
        return security;
    }
}
