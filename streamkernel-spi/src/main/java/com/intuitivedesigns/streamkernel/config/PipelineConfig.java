/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Singleton configuration manager.
 * Loads from -Dsk.config.path or ENV 'SK_CONFIG_PATH'.
 */
public class PipelineConfig {

    private static final PipelineConfig INSTANCE = new PipelineConfig();
    private final Properties props = new Properties();

    private PipelineConfig() {
        load();
    }

    public static PipelineConfig get() {
        return INSTANCE;
    }

    private void load() {
        // 1. Try System Property first (Passed via -Dsk.config.path)
        String path = System.getProperty("sk.config.path");

        // 2. Fallback to Environment Variable
        if (path == null || path.isBlank()) {
            path = System.getenv("SK_CONFIG_PATH");
        }

        if (path != null && !path.isBlank()) {
            System.out.println(">> Loading configuration from: " + path);
            try (InputStream is = new FileInputStream(path)) {
                props.load(is);
                System.out.println(">> Loaded " + props.size() + " properties.");
            } catch (IOException e) {
                System.err.println("!! FAILED to load config file: " + path);
                e.printStackTrace();
            }
        } else {
            System.err.println("!! WARNING: No configuration file specified. Usage: -Dsk.config.path=/path/to/config.properties");
        }
    }

    public String getString(String key, String defaultValue) {
        return props.getProperty(key, defaultValue);
    }

    public int getInt(String key, int defaultValue) {
        String val = props.getProperty(key);
        if (val == null) return defaultValue;
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public long getLong(String key, long defaultValue) {
        String val = props.getProperty(key);
        if (val == null) return defaultValue;
        try {
            return Long.parseLong(val);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        String val = props.getProperty(key);
        return val == null ? defaultValue : Boolean.parseBoolean(val);
    }

    public boolean hasPath(String key) {
        return props.containsKey(key);
    }

    public Map<String, Object> asMap() {
        Map<String, Object> map = new HashMap<>();
        for (String name : props.stringPropertyNames()) {
            map.put(name, props.getProperty(name));
        }
        return map;
    }

    public Set<String> keys() {
        return props.stringPropertyNames();
    }
}