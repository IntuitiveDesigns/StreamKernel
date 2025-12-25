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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class PluginRegistry {

    private final Map<PluginKind, Map<String, PipelinePlugin>> plugins = new ConcurrentHashMap<>();

    public PluginRegistry(ClassLoader cl) {
        for (PluginKind k : PluginKind.values()) {
            plugins.put(k, new ConcurrentHashMap<>());
        }

        ServiceLoader<PipelinePlugin> loader = ServiceLoader.load(PipelinePlugin.class, cl);
        for (PipelinePlugin p : loader) {
            String id = normalize(p.id());
            plugins.get(p.kind()).put(id, p);
        }
    }

    public PipelinePlugin require(PluginKind kind, String id) {
        String key = normalize(id);
        PipelinePlugin p = plugins.get(kind).get(key);
        if (p == null) {
            throw new IllegalArgumentException(
                    "No plugin found. kind=" + kind + " id=" + id + " available=" + plugins.get(kind).keySet()
            );
        }
        return p;
    }

    public Set<String> available(PluginKind kind) {
        return Collections.unmodifiableSet(plugins.get(kind).keySet());
    }

    private static String normalize(String s) {
        return s == null ? "" : s.trim().toUpperCase(Locale.ROOT);
    }
}
