/*
 * Copyright 2025 Steven Lopez
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.example.streamkernel.kafka.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;

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

    // --- REQUIRED FOR KAFKA DRIVER PASSTHROUGH ---
    public Properties getProperties() {
        return this.properties;
    }

    // --- REQUIRED FOR ITERATING SECURITY KEYS ---
    public Set<String> keys() {
        return properties.stringPropertyNames();
    }
}