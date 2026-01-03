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

package com.intuitivedesigns.streamkernel.plugins;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.config.PipelineFactory;
import com.intuitivedesigns.streamkernel.core.OutputSink;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.output.KafkaDlqBytesSink;
import com.intuitivedesigns.streamkernel.spi.DlqSerializer;
import com.intuitivedesigns.streamkernel.spi.SinkPlugin;

public final class KafkaDlqSinkPlugin implements SinkPlugin {

    @Override
    public String id() {
        return "KAFKA_DLQ";
    }

    @Override
    @SuppressWarnings("unchecked")
    public OutputSink<?> create(PipelineConfig config, MetricsRuntime metrics) throws Exception {
        String topic = config.getString("dlq.topic", "streamkernel-dlq");

        DlqSerializer<?> serializer = PipelineFactory.createDlqSerializer(config, metrics);

        // Raw cast is acceptable here because serializer selection is config-driven and DLQ is an edge sink.
        return new KafkaDlqBytesSink<>(topic, KafkaDlqBytesSink.buildProducerProps(config), (DlqSerializer<Object>) serializer);
    }
}



