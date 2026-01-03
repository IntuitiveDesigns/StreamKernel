/*
 * Copyright 2025 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

package com.intuitivedesigns.streamkernel.plugins;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.serializers.StringDlqSerializer;
import com.intuitivedesigns.streamkernel.spi.DlqSerializer;
import com.intuitivedesigns.streamkernel.spi.DlqSerializerPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class StringDlqSerializerPlugin implements DlqSerializerPlugin {

    private static final Logger log = LoggerFactory.getLogger(StringDlqSerializerPlugin.class);

    @Override
    public String id() {
        return "STRING";
    }

    @Override
    public DlqSerializer<?> create(PipelineConfig config, MetricsRuntime metrics) {
        log.info("🔌 Initialized String DLQ Serializer (Format: UTF-8 Bytes)");

        // Explicitly type as <Object> to signal it accepts any payload
        return new StringDlqSerializer<Object>();
    }
}