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

package com.example.streamkernel.kafka.transform;

import com.example.streamkernel.kafka.core.PipelinePayload;
import com.example.streamkernel.kafka.core.Transformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class EtlTransformer implements Transformer<String, String> {

    private static final Logger log = LoggerFactory.getLogger(EtlTransformer.class);

    // Regex to find emails
    private static final Pattern EMAIL_PATTERN = Pattern.compile("([a-zA-Z0-9._-]+@[a-z]+\\.[a-z]+)");

    @Override
    public PipelinePayload<String> transform(PipelinePayload<String> input) {
        // Switched from System.out to Log to keep console clean for the Speedometer
        if (log.isDebugEnabled()) {
            log.debug("[ETL] Transforming ID: {}", input.id());
        }

        // 1. Data Enrichment & Security
        String rawData = input.data();

        // Replaces "bob@gmail.com" with "***@***.com"
        String maskedData = (rawData != null)
                ? EMAIL_PATTERN.matcher(rawData).replaceAll("***@***.com")
                : null;

        // 2. Metadata Enrichment
        // FIX: Changed .metadata() to .headers()
        Map<String, String> newHeaders = new HashMap<>(input.headers());
        newHeaders.put("processed_by", "EtlTransformer");
        newHeaders.put("privacy_check", "PASSED");

        // Return new payload with modified data and headers
        return new PipelinePayload<>(
                input.id(),
                maskedData,
                input.timestamp(),
                newHeaders
        );
    }
}