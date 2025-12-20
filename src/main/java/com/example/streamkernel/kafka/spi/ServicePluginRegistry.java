package com.example.streamkernel.kafka.spi;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

public final class ServicePluginRegistry<T> {

    private final Map<String, T> byId;

    public interface IdExtractor<T> {
        String id(T plugin);
    }

    public ServicePluginRegistry(Class<T> spiType, ClassLoader cl, IdExtractor<T> idExtractor) {
        Map<String, T> tmp = new LinkedHashMap<>();
        ServiceLoader<T> loader = ServiceLoader.load(spiType, cl);
        for (T plugin : loader) {
            String id = PluginIds.normalize(idExtractor.id(plugin));
            if (id.isEmpty()) {
                throw new IllegalStateException("Plugin id() must not be blank for " + plugin.getClass().getName());
            }
            if (tmp.containsKey(id)) {
                throw new IllegalStateException(
                        "Duplicate plugin id '" + id + "' for SPI " + spiType.getName()
                                + " plugins: " + tmp.get(id).getClass().getName()
                                + " and " + plugin.getClass().getName()
                );
            }
            tmp.put(id, plugin);
        }
        this.byId = Collections.unmodifiableMap(tmp);
    }

    public T require(String id, String configKeyName) {
        String key = PluginIds.normalize(id);
        T plugin = byId.get(key);
        if (plugin == null) {
            throw new IllegalArgumentException(
                    "No plugin found for '" + configKeyName + "=" + id + "'. Available: " + byId.keySet()
            );
        }
        return plugin;
    }

    public Set<String> availableIds() {
        return byId.keySet();
    }
}
