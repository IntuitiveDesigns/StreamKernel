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

package com.example.streamkernel.kafka.plugins;

import com.example.streamkernel.kafka.config.PipelineConfig;
import com.example.streamkernel.kafka.core.OutputSink;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;
import com.example.streamkernel.kafka.spi.SinkPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DlqLogSinkPlugin implements SinkPlugin {

    private static final Logger log = LoggerFactory.getLogger(DlqLogSinkPlugin.class);

    @Override
    public String id() {
        return "DLQ_LOG";
    }

    @Override
    public OutputSink<String> create(PipelineConfig config, MetricsRuntime metrics) {
        return payload -> log.warn("⚠️ [DLQ] Dropped Event ID: {}", payload.id());
    }
}
