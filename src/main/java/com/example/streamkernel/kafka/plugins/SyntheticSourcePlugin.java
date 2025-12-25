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

import com.example.streamkernel.kafka.bench.SyntheticSource;
import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.core.SourceConnector;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;
import com.example.streamkernel.kafka.spi.SourcePlugin;

public final class SyntheticSourcePlugin implements SourcePlugin {

    @Override
    public String id() {
        return "SYNTHETIC";
    }

    @Override
    public SourceConnector<String> create(PipelineConfig config, MetricsRuntime metrics) {
        int payloadSize = Integer.parseInt(config.getProperty("source.synthetic.payload.size", "1024"));
        boolean highEntropy = Boolean.parseBoolean(config.getProperty("source.synthetic.high.entropy", "false"));
        return new SyntheticSource(payloadSize, highEntropy);
    }
}
