package com.example.streamkernel.kafka.config;

import com.example.streamkernel.kafka.core.CacheStrategy;
import com.example.streamkernel.kafka.core.OutputSink;
import com.example.streamkernel.kafka.core.SourceConnector;
import com.example.streamkernel.kafka.core.Transformer;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;
import com.example.streamkernel.kafka.spi.PluginCatalog;
import com.example.streamkernel.kafka.spi.SourcePlugin;
import com.example.streamkernel.kafka.spi.SinkPlugin;
import com.example.streamkernel.kafka.spi.TransformerPlugin;
import com.example.streamkernel.kafka.spi.CachePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PipelineFactory {

    private static final Logger log = LoggerFactory.getLogger(PipelineFactory.class);

    // Loaded once at startup; O(1) lookups afterwards.
    private static final PluginCatalog CATALOG =
            new PluginCatalog(Thread.currentThread().getContextClassLoader());

    private PipelineFactory() {}

    public static SourceConnector<String> createSource(PipelineConfig config, MetricsRuntime metrics) {
        String id = require(config, "source.type");
        SourcePlugin plugin = CATALOG.sources.require(id, "source.type");
        try {
            return plugin.create(config, metrics);
        } catch (Exception e) {
            throw new RuntimeException("Failed creating source plugin id=" + id + " class=" + plugin.getClass().getName(), e);
        }
    }

    public static OutputSink<String> createSink(PipelineConfig config, MetricsRuntime metrics) {
        String id = require(config, "sink.type");
        SinkPlugin plugin = CATALOG.sinks.require(id, "sink.type");
        try {
            return plugin.create(config, metrics);
        } catch (Exception e) {
            throw new RuntimeException("Failed creating sink plugin id=" + id + " class=" + plugin.getClass().getName(), e);
        }
    }

    public static OutputSink<String> createDlq(PipelineConfig config, MetricsRuntime metrics) {
        String id = require(config, "dlq.type");
        SinkPlugin plugin = CATALOG.sinks.require(id, "dlq.type");
        try {
            return plugin.create(config, metrics);
        } catch (Exception e) {
            throw new RuntimeException("Failed creating DLQ sink plugin id=" + id + " class=" + plugin.getClass().getName(), e);
        }
    }

    public static Transformer<String, String> createTransformer(PipelineConfig config, MetricsRuntime metrics) {
        String id = require(config, "transform.type");
        TransformerPlugin plugin = CATALOG.transformers.require(id, "transform.type");
        try {
            return plugin.create(config, metrics);
        } catch (Exception e) {
            throw new RuntimeException("Failed creating transformer plugin id=" + id + " class=" + plugin.getClass().getName(), e);
        }
    }

    public static CacheStrategy<String> createCache(PipelineConfig config, MetricsRuntime metrics) {
        String id = require(config, "cache.type");
        CachePlugin plugin = CATALOG.caches.require(id, "cache.type");
        try {
            return plugin.create(config, metrics);
        } catch (Exception e) {
            throw new RuntimeException("Failed creating cache plugin id=" + id + " class=" + plugin.getClass().getName(), e);
        }
    }

    /** Optional: call once on boot to log what is available. */
    public static void logAvailablePlugins() {
        log.info("Plugins available: sources={} sinks={} transformers={} caches={}",
                CATALOG.sources.availableIds(),
                CATALOG.sinks.availableIds(),
                CATALOG.transformers.availableIds(),
                CATALOG.caches.availableIds());
    }

    private static String require(PipelineConfig config, String key) {
        String v = config.getProperty(key, null);
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException("Missing required config: " + key);
        }
        return v;
    }
}
