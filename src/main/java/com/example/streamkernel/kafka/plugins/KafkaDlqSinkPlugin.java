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
import com.example.streamkernel.kafka.config.PipelineFactory;
import com.example.streamkernel.kafka.core.OutputSink;
import com.example.streamkernel.kafka.metrics.MetricsRuntime;
import com.example.streamkernel.kafka.output.KafkaDlqBytesSink;
import com.example.streamkernel.kafka.spi.DlqSerializer;
import com.example.streamkernel.kafka.spi.SinkPlugin;

public final class KafkaDlqSinkPlugin implements SinkPlugin {

    @Override
    public String id() {
        return "KAFKA_DLQ";
    }

    @Override
    @SuppressWarnings("unchecked")
    public OutputSink<?> create(PipelineConfig config, MetricsRuntime metrics) throws Exception {
        String topic = config.getProperty("dlq.topic", "streamkernel-dlq");

        DlqSerializer<?> serializer = PipelineFactory.createDlqSerializer(config, metrics);

        // Raw cast is acceptable here because serializer selection is config-driven and DLQ is an edge sink.
        return new KafkaDlqBytesSink<>(topic, KafkaDlqBytesSink.buildProducerProps(config), (DlqSerializer<Object>) serializer);
    }
}
