package com.example.streamkernel.kafka.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PipelineConfig {

    private static final Logger log = LoggerFactory.getLogger(PipelineConfig.class);
    private static PipelineConfig INSTANCE;
    private final Properties properties;

    private PipelineConfig(String configPath) {
        this.properties = new Properties();
        loadProperties(configPath);
    }

    public static synchronized PipelineConfig get() {
        if (INSTANCE == null) {
            String path = System.getProperty("pipeline.config", "src/main/resources/pipeline.properties");
            INSTANCE = new PipelineConfig(path);
        }
        return INSTANCE;
    }

    private void loadProperties(String path) {
        // Try file system first
        try (InputStream input = new FileInputStream(path)) {
            properties.load(input);
            return;
        } catch (IOException e) {
            // Ignore, try classpath
        }

        // Try classpath
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("pipeline.properties")) {
            if (input == null) {
                log.warn("⚠️ No pipeline.properties found. Using empty defaults.");
                return;
            }
            properties.load(input);
        } catch (IOException ex) {
            log.error("Failed to load config", ex);
        }
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    public String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public int getInt(String key) {
        String val = properties.getProperty(key);
        if (val == null) throw new IllegalArgumentException("Missing required config: " + key);
        return Integer.parseInt(val);
    }

    // --- THE MISSING METHOD ---
    public Properties getProperties() {
        return this.properties;
    }
}