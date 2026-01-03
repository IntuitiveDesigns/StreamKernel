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
import com.intuitivedesigns.streamkernel.core.Transformer;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.spi.PluginKind;
import com.intuitivedesigns.streamkernel.spi.TransformerPlugin;

import java.util.Locale;
import java.util.Objects;

public final class UppercaseTransformerPlugin implements TransformerPlugin {

    public static final String ID = "UPPER";

    @Override
    public String id() {
        return ID;
    }

    @Override
    public PluginKind kind() {
        return PluginKind.TRANSFORMER;
    }

    @Override
    public Transformer<?, ?> create(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");

        // Locale-safe, allocation-minimal transformer
        return payload -> {
            String data = (String)payload.data();
            if (data == null || data.isEmpty()) {
                return payload;
            }
            return payload.withData(data.toUpperCase(Locale.ROOT));
        };
    }
}
