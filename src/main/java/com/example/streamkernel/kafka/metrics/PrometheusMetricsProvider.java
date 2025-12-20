package com.example.streamkernel.kafka.metrics;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

public final class PrometheusMetricsProvider implements MetricsProvider {

    @Override public String type() { return "PROMETHEUS"; }

    @Override
    public MetricsRuntime create(MetricsSettings s) {
        PrometheusMeterRegistry reg = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        HttpServer server = start(reg, s.prometheusPort);

        return new MetricsRuntime() {
            @Override public MeterRegistry registry() { return reg; }
            @Override public boolean enabled() { return true; }
            @Override public String type() { return "PROMETHEUS"; }

            @Override
            public void close() {
                try { server.stop(0); } catch (Exception ignored) {}
                try { reg.close(); } catch (Exception ignored) {}
            }
        };
    }

    private static HttpServer start(PrometheusMeterRegistry registry, int port) {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/metrics", exchange -> {
                byte[] bytes = registry.scrape().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
                exchange.sendResponseHeaders(200, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) { os.write(bytes); }
            });

            Thread t = new Thread(server::start, "metrics-http-server");
            t.setDaemon(true);
            t.start();

            return server;
        } catch (Exception e) {
            throw new RuntimeException("Failed to start Prometheus metrics server", e);
        }
    }
}
