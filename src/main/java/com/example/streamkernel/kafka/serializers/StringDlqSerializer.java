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

package com.example.streamkernel.kafka.serializers;

import com.example.streamkernel.kafka.core.PipelinePayload;
import com.example.streamkernel.kafka.spi.DlqSerializer;

import java.nio.charset.StandardCharsets;

public final class StringDlqSerializer<I> implements DlqSerializer<I> {

    @Override
    public String id() {
        return "STRING";
    }

    @Override
    public byte[] key(PipelinePayload<I> payload) {
        return payload.id().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] value(PipelinePayload<I> payload) {
        // Minimal dependency approach. If payload.data() is already String, this is zero-copy at the object level,
        // but still creates UTF-8 bytes (required for Kafka byte[] serializer).
        return String.valueOf(payload.data()).getBytes(StandardCharsets.UTF_8);
    }
}
