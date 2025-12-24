# 09 — Validation Matrix

Run order:

1. Baseline throughput (PLAINTEXT)
2. OPA allow/deny + DLQ routing
3. SSL/mTLS connectivity
4. Combine all (SSL + OPA + DLQ)
5. Add Avro / Mongo / transforms as needed

Tip: keep separate `pipeline-*.properties` profile files per scenario.
