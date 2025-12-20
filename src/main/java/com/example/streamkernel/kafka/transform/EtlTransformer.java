package com.example.streamkernel.kafka.transform;

import com.example.streamkernel.kafka.core.PipelinePayload;
import com.example.streamkernel.kafka.core.Transformer;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class EtlTransformer implements Transformer<String, String> {

    // Regex to find emails
    private static final Pattern EMAIL_PATTERN = Pattern.compile("([a-zA-Z0-9._-]+@[a-z]+\\.[a-z]+)");

    @Override
    public PipelinePayload<String> transform(PipelinePayload<String> input) {
        System.out.println("[ETL] Transforming ID: " + input.id());

        // 1. Data Enrichment: Uppercase the content (Simple ETL)
        String rawData = input.data();

        // 2. Security: Mask PII (Emails)
        // Replaces "bob@gmail.com" with "***@***.com"
        String maskedData = EMAIL_PATTERN.matcher(rawData)
                .replaceAll("***@***.com");

        // 3. Metadata Enrichment
        Map<String, String> newMeta = new HashMap<>(input.metadata());
        newMeta.put("processed_by", "EtlTransformer");
        newMeta.put("privacy_check", "PASSED");

        // Return new payload with modified data and metadata
        return new PipelinePayload<>(
                input.id(),
                maskedData,
                input.timestamp(),
                newMeta
        );
    }
}