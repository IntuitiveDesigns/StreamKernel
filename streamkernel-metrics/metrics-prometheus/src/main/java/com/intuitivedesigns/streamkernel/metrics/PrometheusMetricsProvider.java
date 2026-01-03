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

package com.intuitivedesigns.streamkernel.metrics;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public final class PrometheusMetricsProvider implements MetricsProvider {

    private static final Logger log = LoggerFactory.getLogger(PrometheusMetricsProvider.class);

    private static final String PATH = "/metrics";

    @Override
    public String id() {
        return "PROMETHEUS";
    }

    @Override
    public MetricsRuntime create(MetricsSettings s) {
        // SPI contract: return null if not applicable
        if (s == null || !matches(s.providerId)) {
            return null;
        }

        final int port = clampPort(s.prometheusPort);

        final PrometheusMeterRegistry reg = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        MetricsUtil.applyCommonTags(reg, s);

        final ServerHandle handle = start(reg, port);

        log.info("✅ Prometheus Metrics Active (port={}, path={})", port, PATH);

        return new MetricsRuntime() {
            @Override public MeterRegistry registry() { return reg; }
            @Override public boolean enabled() { return true; }
            @Override public String type() { return "PROMETHEUS"; }

            @Override
            public void close() {
                try { handle.close(); } catch (Exception ignored) {}
                try { reg.close(); } catch (Exception ignored) {}
            }
        };
    }

    private static ServerHandle start(PrometheusMeterRegistry registry, int port) {
        Objects.requireNonNull(registry, "registry");

        try {
            final HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);

            final ExecutorService executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "metrics-http-server");
                    t.setDaemon(true);
                    return t;
                }
            });
            server.setExecutor(executor);

            server.createContext(PATH, exchange -> {
                try {
                    final byte[] bytes = registry.scrape().getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
                    exchange.sendResponseHeaders(200, bytes.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(bytes);
                    }
                } catch (Exception e) {
                    try { exchange.sendResponseHeaders(500, -1); } catch (Exception ignored) {}
                } finally {
                    try { exchange.close(); } catch (Exception ignored) {}
                }
            });

            server.start();
            return new ServerHandle(server, executor);
        } catch (Exception e) {
            throw new RuntimeException("Failed to start Prometheus metrics server on port " + port, e);
        }
    }

    private static int clampPort(int port) {
        if (port <= 0) return 9090;
        if (port > 65_535) return 65_535;
        return port;
    }

    private static final class ServerHandle implements AutoCloseable {
        private final HttpServer server;
        private final ExecutorService executor;

        private ServerHandle(HttpServer server, ExecutorService executor) {
            this.server = server;
            this.executor = executor;
        }

        @Override
        public void close() {
            try { server.stop(0); } catch (Exception ignored) {}
            try { executor.shutdownNow(); } catch (Exception ignored) {}
        }
    }
}
