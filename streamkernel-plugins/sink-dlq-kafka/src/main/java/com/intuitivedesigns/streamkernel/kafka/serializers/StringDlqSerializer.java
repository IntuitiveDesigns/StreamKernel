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
import com.intuitivedesigns.streamkernel.spi.PluginKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public final class StringDlqSerializerPlugin implements DlqSerializerPlugin {

    private static final Logger log = LoggerFactory.getLogger(StringDlqSerializerPlugin.class);

    // Config keys
    private static final String CFG_LOG_INIT = "dlq.serializer.string.log.init";

    // Defaults
    private static final boolean DEFAULT_LOG_INIT = true;

    @Override
    public String id() {
        return "STRING";
    }

    @Override
    public PluginKind kind() {
        return PluginKind.DLQ_SERIALIZER;
    }

    @Override
    public DlqSerializer<?> create(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");

        final boolean logInit = parseBoolean(config.getString(CFG_LOG_INIT, Boolean.toString(DEFAULT_LOG_INIT)));
        if (logInit && log.isInfoEnabled()) {
            log.info("Initialized STRING DLQ Serializer (UTF-8 bytes, value=String.valueOf(payload.data()))");
        }

        // Accept any payload type; serializer decides how to stringify.
        return new StringDlqSerializer<>();
    }

    private static boolean parseBoolean(String s) {
        if (s == null) return false;
        return Boolean.parseBoolean(s.trim());
    }
}
