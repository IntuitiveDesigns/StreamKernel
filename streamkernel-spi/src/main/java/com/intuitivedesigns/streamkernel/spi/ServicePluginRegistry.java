/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */
package com.intuitivedesigns.streamkernel.spi;

import java.util.*;

/**
 * Universal Registry for SPI discovery.
 *
 * <p><b>Performance Note:</b> This class performs the expensive ServiceLoader
 * classpath scan <b>once</b> at startup and caches the results. Subsequent
 * lookups are O(1).</p>
 *
 * @param <T> The SPI interface type (e.g., SourcePlugin.class)
 */

public final class ServicePluginRegistry<T extends PipelinePlugin<?>> {
    private final Map<String, T> byId;

    public ServicePluginRegistry(Class<T> spiType) {
        // Use the thread's context classloader (standard for webapps/frameworks)
        this(spiType, Thread.currentThread().getContextClassLoader());
    }

    public ServicePluginRegistry(Class<T> spiType, ClassLoader cl) {
        Map<String, T> tmp = new LinkedHashMap<>();
        ServiceLoader<T> loader = ServiceLoader.load(spiType, cl);
        for (T plugin : loader) {
            // We can now call .id() directly because T extends PipelinePlugin
            String id = PluginIds.normalize(plugin.id());
            if (id.isEmpty()) {
                throw new IllegalStateException("Plugin id() must not be blank for " + plugin.getClass().getName());
            }
            if (tmp.containsKey(id)) {
                throw new IllegalStateException("Duplicate plugin ID '" + id + "' for SPI " + spiType.getSimpleName() + ". Conflict between: " + tmp.get(id).getClass().getName() + " and " + plugin.getClass().getName());
            }
            tmp.put(id, plugin);
        }
        this.byId = Collections.unmodifiableMap(tmp);
    }

    public T require(String id, String configKeyName) {
        String key = PluginIds.normalize(id);
        T plugin = byId.get(key);
        if (plugin == null) {
            throw new IllegalArgumentException("No plugin found for '" + configKeyName + "=" + id + "'. " + "Available options: " + byId.keySet());
        }
        return plugin;
    }

    public Set<String> availableIds() {
        return byId.keySet();
    }

    public Optional<T> get(String id) {
        return Optional.ofNullable(byId.get(PluginIds.normalize(id)));
    }
}