package com.example.streamkernel.kafka.spi;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class PluginRegistry {

    private final Map<PluginKind, Map<String, PipelinePlugin>> plugins = new ConcurrentHashMap<>();

    public PluginRegistry(ClassLoader cl) {
        for (PluginKind k : PluginKind.values()) {
            plugins.put(k, new ConcurrentHashMap<>());
        }

        ServiceLoader<PipelinePlugin> loader = ServiceLoader.load(PipelinePlugin.class, cl);
        for (PipelinePlugin p : loader) {
            String id = normalize(p.id());
            plugins.get(p.kind()).put(id, p);
        }
    }

    public PipelinePlugin require(PluginKind kind, String id) {
        String key = normalize(id);
        PipelinePlugin p = plugins.get(kind).get(key);
        if (p == null) {
            throw new IllegalArgumentException(
                    "No plugin found. kind=" + kind + " id=" + id + " available=" + plugins.get(kind).keySet()
            );
        }
        return p;
    }

    public Set<String> available(PluginKind kind) {
        return Collections.unmodifiableSet(plugins.get(kind).keySet());
    }

    private static String normalize(String s) {
        return s == null ? "" : s.trim().toUpperCase(Locale.ROOT);
    }
}
