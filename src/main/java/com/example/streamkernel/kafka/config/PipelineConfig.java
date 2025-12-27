/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */
package com.example.streamkernel.kafka.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public final class PipelineConfig {

    private static final Logger log = LoggerFactory.getLogger(PipelineConfig.class);
    private static volatile PipelineConfig INSTANCE;

    public static final String ENV_PREFIX = "SK_";
    public static final String ENV_CONFIG_PATH = "SK_CONFIG_PATH";
    public static final String SYS_CONFIG_PATH = "pipeline.config";

    // Container-first default; classpath fallback still supported.
    public static final String DEFAULT_CONFIG_PATH = "/etc/streamkernel/config/pipeline.properties";

    private final Properties fileProperties = new Properties();
    private final String loadedFrom;

    private PipelineConfig(String configPath) {
        this.loadedFrom = loadProperties(configPath);
    }

    public static PipelineConfig get() {
        PipelineConfig local = INSTANCE;
        if (local == null) {
            synchronized (PipelineConfig.class) {
                local = INSTANCE;
                if (local == null) {
                    String path = firstNonBlank(System.getenv(ENV_CONFIG_PATH), System.getProperty(SYS_CONFIG_PATH), DEFAULT_CONFIG_PATH);
                    INSTANCE = local = new PipelineConfig(path);
                }
            }
        }
        return local;
    }

    private String loadProperties(String configPath) {
        // 1) Filesystem path (ConfigMap mount / local absolute path)
        if (configPath != null && !configPath.isBlank()) {
            Path p = Path.of(configPath);
            if (Files.exists(p) && Files.isRegularFile(p)) {
                try (InputStream in = Files.newInputStream(p)) {
                    fileProperties.load(in);
                    log.info("✅ Loaded configuration from file: {}", configPath);
                    return "file:" + configPath;
                } catch (IOException e) {
                    log.warn("⚠️ Failed to load config from file: {}", configPath, e);
                }
            }
        }

        // 2) Classpath fallback - try the same resource name if possible
        String resourceName = (configPath != null && configPath.contains("/"))
                ? configPath.substring(configPath.lastIndexOf('/') + 1)
                : (configPath == null || configPath.isBlank() ? "pipeline.properties" : configPath);

        try (InputStream in = PipelineConfig.class.getClassLoader().getResourceAsStream(resourceName)) {
            if (in != null) {
                fileProperties.load(in);
                log.info("✅ Loaded configuration from classpath: {}", resourceName);
                return "classpath:" + resourceName;
            }
        } catch (IOException e) {
            log.warn("⚠️ Failed to load config from classpath: {}", resourceName, e);
        }

        log.warn("⚠️ No config found (file or classpath). Using empty defaults.");
        return "none";
    }

    public String source() {
        return loadedFrom;
    }

    public String getProperty(String key) {
        return getProperty(key, null);
    }

    public String getProperty(String key, String defaultValue) {
        // 1) Env var override
        String envKey = toEnvKey(key);
        String envVal = System.getenv(envKey);
        if (isNonBlank(envVal)) return envVal;

        // 2) System property override
        String sysVal = System.getProperty(key);
        if (isNonBlank(sysVal)) return sysVal;

        // 3) File property
        return fileProperties.getProperty(key, defaultValue);
    }

    public int getInt(String key) {
        String v = getProperty(key);
        if (v == null) throw new IllegalArgumentException("Missing required config: " + key);
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid int for " + key + ": " + v, e);
        }
    }

    /** Export only StreamKernel keys, not every SK_* env var. */
    public Properties mergedProperties() {
        Properties merged = new Properties();
        merged.putAll(fileProperties);

        // Overlay env vars that map to keys present in file OR known prefixes.
        // This avoids accidentally injecting unrelated SK_* vars.
        for (Map.Entry<String, String> e : System.getenv().entrySet()) {
            String k = e.getKey();
            String v = e.getValue();
            if (!k.startsWith(ENV_PREFIX) || !isNonBlank(v)) continue;

            String propKey = fromEnvKey(k);
            // Allow env override for keys that exist in file, or that are under known prefixes
            if (fileProperties.containsKey(propKey) || propKey.startsWith("kafka.") || propKey.startsWith("security.") || propKey.startsWith("metrics.") || propKey.startsWith("dlq.")) {
                merged.put(propKey, v);
            }
        }

        // Optionally overlay system properties for the same prefixes
        // (kept minimal to avoid surprises; can be expanded later)
        return merged;
    }

    /** Kafka client properties only (clean, avoids polluting kafka client config). */
    public Properties kafkaClientProperties() {
        Properties all = mergedProperties();
        Properties kafka = new Properties();

        for (String name : all.stringPropertyNames()) {
            if (name.startsWith("kafka.")) {
                kafka.put(name.substring("kafka.".length()), all.getProperty(name));
            }
        }
        return kafka;
    }

    public Set<String> keys() {
        return fileProperties.stringPropertyNames();
    }

    private static String toEnvKey(String propKey) {
        return ENV_PREFIX + propKey.replace('.', '_').toUpperCase(Locale.ROOT);
    }

    private static String fromEnvKey(String envKey) {
        // SK_KAFKA_SSL_KEYSTORE_LOCATION -> kafka.ssl.keystore.location
        String raw = envKey.substring(ENV_PREFIX.length()).toLowerCase(Locale.ROOT);
        return raw.replace('_', '.');
    }

    private static boolean isNonBlank(String s) {
        return s != null && !s.isBlank();
    }

    private static String firstNonBlank(String... vals) {
        for (String v : vals) if (isNonBlank(v)) return v;
        return null;
    }

    // Optional for tests
    static void resetForTests() { INSTANCE = null; }
}
