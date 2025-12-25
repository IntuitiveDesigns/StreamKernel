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
import com.example.streamkernel.kafka.output.KafkaAvroSink;
import com.example.streamkernel.kafka.spi.SinkPlugin;

public final class KafkaAvroSinkPlugin implements SinkPlugin {

    @Override
    public String id() {
        return "KAFKA_AVRO";
    }

    @Override
    public OutputSink<String> create(PipelineConfig config, MetricsRuntime metrics) {
        String topic = config.getProperty("sink.topic", "streamkernel-avro-default");
        // Delegate to the Sink class, matching your KafkaSink style
        return KafkaAvroSink.fromConfig(config, topic, metrics);
    }
}