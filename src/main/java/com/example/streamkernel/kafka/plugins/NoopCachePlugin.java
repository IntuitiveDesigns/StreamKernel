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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package com.example.streamkernel.kafka.plugins;

import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.core.CacheStrategy;
import com.example.streamkernel.kafka.core.PipelinePayload;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;
import com.example.streamkernel.kafka.spi.CachePlugin;

import java.util.Optional;

public final class NoopCachePlugin implements CachePlugin {

    @Override
    public String id() {
        return "NOOP";
    }

    @Override
    public CacheStrategy<String> create(PipelineConfig config, MetricsRuntime metrics) {
        return new CacheStrategy<>() {
            @Override public void put(String key, PipelinePayload<String> value) {}
            @Override public Optional<PipelinePayload<String>> get(String key) { return Optional.empty(); }
            @Override public void remove(String key) {}
        };
    }
}
