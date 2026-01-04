## Profiles

Profiles live under `config/profiles/` and are intended to be runnable, copy/paste configurations.

| Profile | Kafka Transport | OPA RBAC | Sink | Transform | Metrics | Primary Use |
|---|---|---:|---|---|---|---|
| `bench-plaintext.properties` | PLAINTEXT `:9092` | No | Kafka (String) | NOOP | Prometheus | Peak throughput baselines |
| `bench-mtls.properties` | mTLS/SSL `:9093` | No | Kafka (String) | NOOP | Prometheus | Throughput with encryption |
| `secure-durable-mtls-opa.properties` | mTLS/SSL `:9093` | Yes | Kafka (String) | NOOP | Prometheus | “Enterprise posture” run |
| `avro-schema-registry.properties` | PLAINTEXT `:9092` | No | Kafka (Avro) | NOOP | Prometheus | Schema Registry + Avro |
| `mongodb-vector.properties` | N/A (source synthetic by default) | Optional | MongoDB | AI_ENRICHMENT | Prometheus | Vector write path smoke test |
| `metrics-datadog.properties` | PLAINTEXT `:9092` | No | Kafka (String) | NOOP | Datadog | Swap Prom/Grafana for DD |


---

## Configuration Profiles (config/profiles)

| Profile | Purpose | Security | Kafka | OPA | DLQ | Observability |
|------|--------|---------|------|-----|-----|--------------|
| `01-synthetic-bench.properties` | Max throughput benchmark | ❌ | PLAINTEXT | ❌ | ❌ | ✅ |
| `02-opa-secure.properties` | Authorization testing | ❌ | PLAINTEXT | ✅ | ❌ | ✅ |
| `03-mtls-kafka.properties` | mTLS validation | ✅ | SSL | ❌ | ❌ | ✅ |
| `04-dlq-durable.properties` | Failure handling | ❌ | PLAINTEXT | ❌ | ✅ | ✅ |
| `05-avro-schema.properties` | Schema Registry / Avro | ❌ | PLAINTEXT | ❌ | ❌ | ❌ |
| `06-mongodb-vector.properties` | MongoDB Vector sink | ❌ | N/A | ❌ | ❌ | ❌ |
| `07-transforms.properties` | Transform validation | ❌ | DEVNULL | ❌ | ❌ | ❌ |
| `08-full-enterprise.properties` | Production-grade pipeline | ✅ | SSL | ✅ | ✅ | ✅ |

---
### Running a profile

Example: 
.\gradlew.bat :streamkernel-app:run "-Dsk.config.path=C:\workspace\StreamKernel\config\profiles\01_synthetic-bench.properties"