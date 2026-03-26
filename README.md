# StreamKernel
[![Architected by Steven Lopez](https://img.shields.io/badge/Architected%20by-Steven%20Lopez-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/steve-lopez-b9941/)
---
![Java 21](https://img.shields.io/badge/Java-21-orange?style=for-the-badge&logo=java)
![Kafka](https://img.shields.io/badge/Kafka-3.6-black?style=for-the-badge&logo=apachekafka)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue?style=for-the-badge)](https://github.com/IntuitiveDesigns/StreamKernel/blob/main/LICENSE)
[![GitHub all releases](https://img.shields.io/github/downloads/IntuitiveDesigns/StreamKernel/total?style=for-the-badge&label=Downloads)](https://github.com/IntuitiveDesigns/StreamKernel/releases)
[![GitHub stars](https://img.shields.io/github/stars/IntuitiveDesigns/StreamKernel?style=for-the-badge)](https://github.com/IntuitiveDesigns/StreamKernel/stargazers)

![StreamKernel logo](assets/StreamKernel-logo.png)

# StreamKernel

---

StreamKernel is a **Java 21 event orchestration engine** that runs intelligence intrinsic to the pipeline — policy enforcement, in-process AI inference, semantic caching, and multi-destination sinks — all inside a single JVM, deployed as a single JAR, configured from a single properties file.

> **When nothing leaves the process, there's nothing to slow it down.**

---

# One Engine. Many Pipelines.

All benchmarks below are executed using the same StreamKernel runtime,
with different pipeline configurations:

- AI Inference (CPU/GPU)
- Vector Database Enrichment
- HTTP Model Serving Comparison
- Kafka Ingestion / Sink
- Exactly-Once Delivery
- DLQ + Resilience
- OPA Policy Enforcement
- Backpressure Saturation
- Resource-Bounded Deployments

- No architectural rewrites.
- No separate services.
- No microservice orchestration.

Just configuration-driven pipelines.

---

## Measured Performance

All benchmarks were run on an Intel i9-8950HK laptop (6 cores / 12 threads, 32GB RAM) against a local Docker environment. No cloud instances. No dedicated server hardware.

| Profile | Avg Throughput | Records | Delivery | Notes |
|---|---|---|---|---|
| Kafka Bench (NOOP) | **956K ops/sec** | 563M | At-least-once | Raw Kafka producer ceiling |
| Kafka ALO (WireEvent) | **525K ops/sec** | 313M | At-least-once | With transform, 512-byte payload |
| Kafka EOS (WireEvent) | **507K ops/sec** | 301M | Exactly-once | -3.5% cost vs ALO |
| mTLS + OPA (NOOP) | **366K ops/sec** | 217M | At-least-once | TLSv1.3 + OPA `fail.open=false`, 1,024-byte payload |
| MongoDB Insert | **163K docs/sec** | 95.5M | At-least-once | Pure insertMany, WiredTiger ceiling |

**100% pipeline integrity across all runs. Zero data loss. Zero Full GC events.**

> Full benchmark reports, GC logs, Grafana dashboards, and competitive analysis documents are available on request.
> Contact: [lopezstevie@gmail.com](mailto:lopezstevie@gmail.com) · [LinkedIn](https://www.linkedin.com/in/steve-lopez-b9941/)

---

## What This Repository Contains

This public repository contains the **StreamKernel open-source core** under Apache 2.0.

```
streamkernel-spi/          # Plugin interfaces: SinkPlugin, SourcePlugin, TransformerPlugin, etc.
streamkernel-core/         # Pipeline orchestrator, backpressure, DLQ, security context
streamkernel-kafka/        # Kafka source/sink implementation
streamkernel-metrics/      # Prometheus metrics provider, Micrometer integration
streamkernel-plugins/      # Public plugins: sink-kafka, sink-mongo-vector, sink-mongo-insert,
                           #   source-synthetic, transform-etl, transformer-string-to-wireevent,
                           #   cache-local (Caffeine), cache-noop, sink-dlq-kafka, security-opa
streamkernel-app/          # Main application entry point, SPI discovery, fat JAR assembly
config/                    # Pipeline profile .properties files
policies/                  # OPA Rego policy examples
scripts/                   # Demo scripts, benchmark runners, cert generation
docker-compose.yaml        # Kafka + MongoDB + Prometheus + Grafana local stack
```

---

## Private / Enterprise Modules

The following capabilities are implemented in a private repository and are available under a **commercial license**. The public repo demonstrates the execution model; these modules extend it into production AI and regulated deployment scenarios.

| Module | Description |
|---|---|
| **DJL_EMBEDDING transform** | In-process ONNX inference via Deep Java Library. Produces 384-dimensional sentence embeddings (all-MiniLM-L6-v2) without a model server or network hop. |
| **Advanced OPA enforcement** | Per-batch policy evaluation with full Rego rule support, audit logging, and fail-open/closed configuration. Extends the public `security-opa` plugin. |
| **Advanced mTLS profiles** | Production-grade mutual TLS benchmark profiles with certificate rotation support. Extends the public mTLS configuration. |
| **Full benchmark suite** | `test-java-runner.ps1`, GC log analysis, Grafana dashboard JSON (13 panels), benchmark CSV matrix, and meta.json output. Includes all profiles listed below. |
| **Caffeine semantic cache** | Vector-aware semantic caching layer. Cache hits bypass ONNX inference entirely, serving results at in-process memory bandwidth. |
| **GPU inference profiles** | TIER_C GPU pipeline profiles using ONNX Runtime CUDA provider. Requires NVIDIA GPU + CUDA. |

---

## Pipeline Profiles

StreamKernel ships with a comprehensive set of pipeline profiles covering the full operational spectrum:

**Baseline & Throughput**
- `streamkernel_kafka_at_least_once_baseline` — ALO throughput ceiling with WireEvent transform
- `streamkernel_kafka_exactly_once_baseline` — EOS throughput with transaction overhead measurement
- `streamkernel_synthetic-devnull-max` — Raw orchestrator ceiling, no sink overhead
- `streamkernel_source_baseline_uncorked` — Source throughput without downstream pressure
- `streamkernel_mongodb_insert_baseline` — MongoDB WiredTiger write ceiling (pure insertMany)
- `streamkernel_mongodb-vector` — End-to-end embedding → MongoDB Atlas Vector Search pipeline

**AI Inference**
- `streamkernel_sink_kafka_onnx_inproc_e2e_TIER_A` — In-process ONNX inference, CPU, Kafka sink
- `streamkernel_sink_kafka_onnx_inproc_e2e_TIER_C_GPU` — In-process ONNX inference, GPU (CUDA)
- `streamkernel_cache_caffeine_ai_benchmark` — Semantic cache hit/miss throughput measurement
- `streamkernel_sink_kafka_http_tei_e2e_TIER_A` — HTTP embedding via TEI endpoint
- `streamkernel_http_enrich_kafka_sink_TIER_A` — HTTP enrichment pipeline

**Enterprise & Resilience**
- `streamkernel_synthetic_to_kafka_sink_enterprise_mtls_opa` — mTLS + OPA under production load
- `streamkernel_enterprise_opa_resilience` — OPA policy enforcement under failure conditions
- `streamkernel_enterprise_dlq_kafka` — Dead Letter Queue routing and recovery
- `streamkernel_backpressure_saturation` — Backpressure behavior at sink saturation
- `streamkernel_resource_bounded_deployability` — Constrained heap / thread operation

---

## Architecture

```
┌─────────────────────────────── Single JVM Process ────────────────────────────────────┐
│                                                                                        │
│  ┌──────────┐    ┌──────────────────┐    ┌─────────────────────┐    ┌──────────────┐ │
│  │ SOURCES  │───▶│  ORCHESTRATOR    │───▶│    TRANSFORMS       │───▶│    SINKS     │ │
│  │          │    │                  │    │                     │    │              │ │
│  │ Kafka    │    │ 12 workers       │    │ STRING_TO_WIREEVENT │    │ Kafka        │ │
│  │ Synthetic│    │ 500 rec/batch    │    │ DJL_EMBEDDING       │    │ MONGO_INSERT │ │
│  │ REST     │    │ Backpressure     │    │ Caffeine Cache      │    │ MONGO_VECTOR │ │
│  │ (plugin) │    │ DLQ routing      │    │ ETL transforms      │    │ DevNull      │ │
│  └──────────┘    │ OPA enforcement  │    │ (plugin)            │    │ (plugin)     │ │
│                  │ mTLS             │    └─────────────────────┘    └──────────────┘ │
│                  └──────────────────┘                                                 │
│                                                                                        │
│  ┌────────────────────────────────────────────────────────────────────────────────┐   │
│  │  OBSERVABILITY: Prometheus /metrics · Grafana dashboards · GC logs · meta.json │   │
│  └────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                        │
└────────────────────── Single .properties file · Zero cluster dependency ───────────────┘
```

**No cluster manager. No coordinator node. No external inference service.**
Every component in the diagram runs in the same heap, communicates at memory bandwidth, and is configured from one file.

---

## Quick Start

```bash
# Start the local stack (Kafka, MongoDB, Prometheus, Grafana)
docker compose up -d

# Build
./gradlew clean build -x test

# Run a profile
java -jar streamkernel-app/build/libs/streamkernel-app-*-all.jar \
  -Dsk.config.path=config/pipelines/streamkernel_kafka_at_least_once_baseline.properties
```

See [PROFILES.md](PROFILES.md) for the full list of available profiles and configuration options.
See [PLAYBOOKS.md](PLAYBOOKS.md) for step-by-step operational guides.

---

## Why Not Just Build It Yourself?

The DIY equivalent — wiring together the MongoDB Java driver, Deep Java Library, OPA4J, Micrometer, Caffeine, and a hand-rolled batch orchestrator — is estimated at **18–29 weeks** of senior engineering time to reach parity, before accounting for the integration failures, GC tuning, and batch copy semantics that only surface under production load.

StreamKernel has already made those mistakes and fixed them. That institutional knowledge ships with the JAR.

> **The DIY Tax — a full technical breakdown** is available on request.

---

## Licensing

### Community Edition · Apache 2.0

This repository. Free to use, modify, and distribute. Suitable for:
- Open-source experimentation and learning
- Internal tooling and non-commercial use
- Evaluation of the StreamKernel execution model
- Building and contributing public plugins

### Professional Edition · Commercial License

Includes private modules: DJL_EMBEDDING, advanced OPA enforcement, advanced mTLS profiles, Caffeine semantic cache, full benchmark suite, and Grafana dashboards. Suitable for:
- Production AI inference pipelines
- Regulated industry deployments (financial services, healthcare, defense)
- Teams who want the benchmark suite for performance validation
- Organizations that need support and guaranteed response times

### Enterprise Edition · Negotiated Contract

Everything in Professional, plus:
- Custom pipeline profile development
- Architecture review and integration support
- Indemnification and IP assignment options
- OEM and embedding rights

---

## Contact

**Steven Lopez** · Technical Leader / Innovator / Solutions Architect

For commercial licensing, enterprise inquiries, or benchmark documentation:

- 📧 [lopezstevie@gmail.com](mailto:lopezstevie@gmail.com)
- 💼 [linkedin.com/in/steve-lopez-b9941](https://www.linkedin.com/in/steve-lopez-b9941/)

---

## Roadmap

### Completed
- ✅ Java 21 Virtual Thread orchestrator
- ✅ SPI plugin system (Sources, Sinks, Transforms, Caches, Security, DLQ)
- ✅ Kafka at-least-once and exactly-once pipelines
- ✅ MongoDB insertMany and Atlas Vector Search sinks
- ✅ In-process ONNX inference via DJL (private)
- ✅ OPA/RBAC per-batch policy enforcement
- ✅ mTLS mutual authentication
- ✅ Caffeine semantic cache (private)
- ✅ Prometheus metrics + Grafana dashboards (private)
- ✅ Full benchmark automation suite (private)

### In Progress
- 🔧 DJL/ONNX inference performance optimization
- 🔧 GPU inference profiles (CUDA / TIER_C)
- 🔧 MongoDB vector pipeline benchmark documentation

### Planned
- 🗺 Kubernetes Helm chart deployment
- 🗺 Kubernetes Operator / CRD (`kubectl apply -f pipeline.yaml`)
- 🗺 Istio service mesh integration
- 🗺 Redis semantic cache plugin
- 🗺 Snowflake and Databricks sink plugins
- 🗺 Commercial licensing portal

---

## Documentation

| Document | Description |
|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | Module structure and design decisions |
| [MODULES.md](MODULES.md) | SPI plugin catalog |
| [PROFILES.md](PROFILES.md) | Pipeline profile reference |
| [PLAYBOOKS.md](PLAYBOOKS.md) | Operational runbooks |
| [METRICS.md](METRICS.md) | Prometheus metric definitions |
| [SECURITY.md](SECURITY.md) | Security model and mTLS/OPA configuration |

---

## License

StreamKernel public core is licensed under the **Apache License, Version 2.0**.

Private modules (DJL_EMBEDDING, advanced OPA, advanced mTLS, benchmark suite, Caffeine semantic cache) are available under a separate commercial license. Contact [lopezstevie@gmail.com](mailto:lopezstevie@gmail.com) for terms.

Copyright © 2025–2026 Steven Lopez.
