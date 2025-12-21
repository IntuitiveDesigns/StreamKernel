package com.example.streamkernel.kafka.config;

import com.example.streamkernel.kafka.core.CacheStrategy;
import com.example.streamkernel.kafka.core.OutputSink;
import com.example.streamkernel.kafka.core.SourceConnector;
import com.example.streamkernel.kafka.core.Transformer;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;
import com.example.streamkernel.kafka.spi.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PipelineFactory {

    private static final Logger log = LoggerFactory.getLogger(PipelineFactory.class);

    // Singleton Catalog: Scans classpath once at startup (Expensive operation done once)
    private static final PluginCatalog CATALOG =
            new PluginCatalog(Thread.currentThread().getContextClassLoader());

    private PipelineFactory() {}

    // --- FACTORY METHODS ---

    public static SourceConnector<?> createSource(PipelineConfig config, MetricsRuntime metrics) {
        String id = require(config, "source.type");
        SourcePlugin plugin = CATALOG.sources.require(id, "source.type");
        try {
            return plugin.create(config, metrics);
        } catch (Exception e) {
            throw new RuntimeException("Failed creating Source [" + id + "]", e);
        }
    }

    public static OutputSink<?> createSink(PipelineConfig config, MetricsRuntime metrics) {
        String id = config.getProperty("sink.type", "DEVNULL"); // Safe default
        SinkPlugin plugin = CATALOG.sinks.require(id, "sink.type");
        try {
            return plugin.create(config, metrics);
        } catch (Exception e) {
            throw new RuntimeException("Failed creating Sink [" + id + "]", e);
        }
    }

    public static OutputSink<?> createDlq(PipelineConfig config, MetricsRuntime metrics) {
        String id = config.getProperty("dlq.type", "DEVNULL");
        SinkPlugin plugin = CATALOG.sinks.require(id, "dlq.type");
        try {
            return plugin.create(config, metrics);
        } catch (Exception e) {
            throw new RuntimeException("Failed creating DLQ [" + id + "]", e);
        }
    }

    public static Transformer<?, ?> createTransformer(PipelineConfig config, MetricsRuntime metrics) {
        String id = config.getProperty("transform.type", "NONE"); // Safe default
        TransformerPlugin plugin = CATALOG.transformers.require(id, "transform.type");
        try {
            return plugin.create(config, metrics);
        } catch (Exception e) {
            throw new RuntimeException("Failed creating Transformer [" + id + "]", e);
        }
    }

    public static CacheStrategy<?> createCache(PipelineConfig config, MetricsRuntime metrics) {
        String id = config.getProperty("cache.type", "NOOP"); // Safe default
        CachePlugin plugin = CATALOG.caches.require(id, "cache.type");
        try {
            return plugin.create(config, metrics);
        } catch (Exception e) {
            throw new RuntimeException("Failed creating Cache [" + id + "]", e);
        }
    }

    // --- UTILITIES ---

    public static void logAvailablePlugins() {
        log.info("🧩 Plugin Catalog Loaded:");
        log.info("   ├─ Sources:      {}", CATALOG.sources.availableIds());
        log.info("   ├─ Sinks:        {}", CATALOG.sinks.availableIds());
        log.info("   ├─ Transformers: {}", CATALOG.transformers.availableIds());
        log.info("   └─ Caches:       {}", CATALOG.caches.availableIds());
    }

    private static String require(PipelineConfig config, String key) {
        String v = config.getProperty(key, null);
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException("Missing required configuration key: " + key);
        }
        return v;
    }
}