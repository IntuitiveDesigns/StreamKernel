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

package com.example.streamkernel.kafka.spi;

import com.example.streamkernel.kafka.core.PipelinePayload;

/**
 * Produces key/value bytes for DLQ persistence.
 *
 * Design goals:
 *  - Zero/low allocation on the hot path where possible
 *  - Works for any payload type I
 *  - Allows future DLQ formats (Avro/Protobuf/JSON) without changing orchestrator
 */
public interface DlqSerializer<I> {
    String id(); // e.g. "STRING", "JSON", "AVRO_GENERIC", "AVRO_SPECIFIC"
    byte[] key(PipelinePayload<I> payload);
    byte[] value(PipelinePayload<I> payload);
}
