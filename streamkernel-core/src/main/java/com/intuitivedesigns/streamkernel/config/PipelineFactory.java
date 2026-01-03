/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.config;

import com.intuitivedesigns.streamkernel.core.OutputSink;
import com.intuitivedesigns.streamkernel.core.SourceConnector;
import com.intuitivedesigns.streamkernel.core.Transformer;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.spi.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public final class PipelineFactory {

    private static final Logger log = LoggerFactory.getLogger(PipelineFactory.class);

    // Config keys
    private static final String KEY_SOURCE_TYPE = "source.type";
    private static final String KEY_SINK_TYPE = "sink.type";
    private static final String KEY_DLQ_TYPE = "dlq.type";
    private static final String KEY_TRANSFORM_TYPE = "transform.type";
    private static final String KEY_CACHE_TYPE = "cache.type";
    private static final String KEY_SECURITY_TYPE = "security.type";
    private static final String KEY_DLQ_SER_TYPE = "dlq.serializer.type";

    // Defaults
    private static final String DEFAULT_SINK = "DEVNULL";
    private static final String DEFAULT_DLQ = "DEVNULL";
    private static final String DEFAULT_TRANSFORM = "NOOP";
    private static final String DEFAULT_CACHE = "NOOP";
    private static final String DEFAULT_DLQ_SERIALIZER = "STRING";
    private static final String DEFAULT_SECURITY = "PERMIT_ALL";

    private static final PluginCatalog CATALOG = new PluginCatalog(resolveClassLoader());

    private PipelineFactory() {}

    // --- FACTORY METHODS ---

    public static SourceConnector<?> createSource(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");

        final String id = require(config, KEY_SOURCE_TYPE);
        final SourcePlugin plugin = CATALOG.sources().require(id, KEY_SOURCE_TYPE);
        return createSafe(plugin, config, metrics, "Source");
    }

    public static OutputSink<?> createSink(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");

        final String id = normalizeId(config.getString(KEY_SINK_TYPE, DEFAULT_SINK), DEFAULT_SINK);
        final SinkPlugin plugin = CATALOG.sinks().require(id, KEY_SINK_TYPE);
        return createSafe(plugin, config, metrics, "Sink");
    }

    public static OutputSink<?> createDlq(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");

        final String id = normalizeId(config.getString(KEY_DLQ_TYPE, DEFAULT_DLQ), DEFAULT_DLQ);
        final SinkPlugin plugin = CATALOG.sinks().require(id, KEY_DLQ_TYPE);
        return createSafe(plugin, config, metrics, "DLQ");
    }

    public static Transformer<?, ?> createTransformer(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");

        final String id = normalizeId(config.getString(KEY_TRANSFORM_TYPE, DEFAULT_TRANSFORM), DEFAULT_TRANSFORM);
        final TransformerPlugin plugin = CATALOG.transformers().require(id, KEY_TRANSFORM_TYPE);
        return createSafe(plugin, config, metrics, "Transformer");
    }

    // This matches what CachePlugin.create() actually returns.
    public static Cache<?, ?> createCache(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");

        final String id = normalizeId(config.getString(KEY_CACHE_TYPE, DEFAULT_CACHE), DEFAULT_CACHE);
        final CachePlugin plugin = CATALOG.caches().require(id, KEY_CACHE_TYPE);
        return createSafe(plugin, config, metrics, "Cache");
    }

    public static DlqSerializer<?> createDlqSerializer(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");

        final String id = normalizeId(config.getString(KEY_DLQ_SER_TYPE, DEFAULT_DLQ_SERIALIZER), DEFAULT_DLQ_SERIALIZER);
        final DlqSerializerPlugin plugin = CATALOG.dlqSerializers().require(id, KEY_DLQ_SER_TYPE);
        return createSafe(plugin, config, metrics, "DLQ Serializer");
    }

    public static SecurityProvider createSecurity(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");

        final String id = normalizeId(config.getString(KEY_SECURITY_TYPE, DEFAULT_SECURITY), DEFAULT_SECURITY);
        final SecurityPlugin plugin = CATALOG.security().require(id, KEY_SECURITY_TYPE);
        return createSafe(plugin, config, metrics, "Security Provider");
    }

    // --- UTILITIES ---

    public static void logAvailablePlugins() {
        log.info("Plugin Catalog Loaded:");
        log.info("  Sources:         {}", CATALOG.sources().availableIds());
        log.info("  Sinks:           {}", CATALOG.sinks().availableIds());
        log.info("  Transformers:    {}", CATALOG.transformers().availableIds());
        log.info("  Caches:          {}", CATALOG.caches().availableIds());
        log.info("  Security:        {}", CATALOG.security().availableIds());
        log.info("  DLQ Serializers: {}", CATALOG.dlqSerializers().availableIds());
    }

    private static String require(PipelineConfig config, String key) {
        final String v = config.getString(key, null);
        if (v == null) {
            throw new IllegalArgumentException("Missing required configuration key: " + key);
        }
        final String s = v.trim();
        if (s.isEmpty()) {
            throw new IllegalArgumentException("Blank value for required configuration key: " + key);
        }
        return s;
    }

    private static String normalizeId(String raw, String fallback) {
        if (raw == null) return fallback;
        final String s = raw.trim();
        return s.isEmpty() ? fallback : s;
    }

    private static ClassLoader resolveClassLoader() {
        final ClassLoader ctx = Thread.currentThread().getContextClassLoader();
        return (ctx != null) ? ctx : PipelineFactory.class.getClassLoader();
    }

    private static <T> T createSafe(PipelinePlugin<T> plugin,
                                    PipelineConfig config,
                                    MetricsRuntime metrics,
                                    String typeName) {
        Objects.requireNonNull(plugin, "plugin");
        try {
            return plugin.create(config, metrics);
        } catch (Throwable t) {
            final String pluginId;
            try {
                pluginId = String.valueOf(plugin.id());
            } catch (Throwable ignored) {
                throw new RuntimeException("Failed creating " + typeName + " (plugin id unavailable)", t);
            }
            throw new RuntimeException("Failed creating " + typeName + " [" + pluginId + "]", t);
        }
    }
}
