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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package com.example.streamkernel.kafka.metrics;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

public final class PrometheusMetricsProvider implements MetricsProvider {

    @Override public String type() { return "PROMETHEUS"; }

    @Override
    public MetricsRuntime create(MetricsSettings s) {
        PrometheusMeterRegistry reg = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

        // Apply consistent tags across the entire app
        MetricsUtil.applyCommonTags(reg, s);

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

            // keep scrape handling deterministic
            server.setExecutor(Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "metrics-http-server");
                t.setDaemon(true);
                return t;
            }));

            server.createContext("/metrics", exchange -> {
                byte[] bytes = registry.scrape().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
                exchange.sendResponseHeaders(200, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) { os.write(bytes); }
            });

            server.start();
            return server;
        } catch (Exception e) {
            throw new RuntimeException("Failed to start Prometheus metrics server", e);
        }
    }
}
