package com.example.streamkernel.kafka.metrics;

import com.example.streamkernel.kafka.config.PipelineConfig;

import java.time.Duration;

public final class MetricsSettings {

    public final boolean enabled;
    public final String type; // PROMETHEUS | DATADOG | NONE

    // Prometheus
    public final int prometheusPort;

    // Datadog
    public final Duration step;
    public final String datadogApiKey;
    public final String datadogUri;
    public final String datadogHostTag;

    private MetricsSettings(
            boolean enabled,
            String type,
            int prometheusPort,
            Duration step,
            String datadogApiKey,
            String datadogUri,
            String datadogHostTag
    ) {
        this.enabled = enabled;
        this.type = type;
        this.prometheusPort = prometheusPort;
        this.step = step;
        this.datadogApiKey = datadogApiKey;
        this.datadogUri = datadogUri;
        this.datadogHostTag = datadogHostTag;
    }

    public static MetricsSettings from(PipelineConfig config) {
        boolean enabled = Boolean.parseBoolean(
                System.getProperty("streamkernel.metrics.enabled",
                        config.getProperty("metrics.enabled", "true"))
        );

        String type = config.getProperty("metrics.type", "PROMETHEUS").trim().toUpperCase();

        int port = Integer.parseInt(config.getProperty("metrics.prometheus.port", "8080"));

        Duration step = Duration.ofSeconds(Integer.parseInt(config.getProperty("metrics.step.seconds", "10")));

        String apiKey = firstNonBlank(System.getenv("DD_API_KEY"), config.getProperty("metrics.datadog.apiKey", ""));
        String uri = config.getProperty("metrics.datadog.uri", "https://api.datadoghq.com");
        String hostTag = config.getProperty("metrics.datadog.hostTag", "host");

        return new MetricsSettings(enabled, type, port, step, apiKey, uri, hostTag);
    }

    private static String firstNonBlank(String a, String b) {
        if (a != null && !a.isBlank()) return a;
        if (b != null && !b.isBlank()) return b;
        return null;
    }
}
