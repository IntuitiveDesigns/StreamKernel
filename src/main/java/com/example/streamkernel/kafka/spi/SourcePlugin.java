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
package com.example.streamkernel.kafka.spi;

import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.core.SourceConnector;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;

public interface SourcePlugin {
    String id(); // e.g. "SYNTHETIC", "KAFKA", "REST"
    SourceConnector<?> create(PipelineConfig config, MetricsRuntime metrics) throws Exception;
}
