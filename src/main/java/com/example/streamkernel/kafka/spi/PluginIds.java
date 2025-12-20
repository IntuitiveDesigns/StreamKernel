package com.example.streamkernel.kafka.spi;

import java.util.Locale;

public final class PluginIds {
    private PluginIds() {}

    public static String normalize(String s) {
        return s == null ? "" : s.trim().toUpperCase(Locale.ROOT);
    }
}
