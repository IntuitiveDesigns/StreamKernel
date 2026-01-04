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

StreamKernel is a high-performance, Kafka-native event orchestration engine designed to decouple **pipeline orchestration** from **payload semantics**.

It models real-world, production-grade streaming workloads including backpressure, enrichment, durability, security, and observability, while sustaining extremely high throughput using Java 21 Virtual Threads and ZGC.

---

## 🚀 Key Features

### Core Architecture
* **Virtual Thread Orchestrator:** Uses Java 21's `newVirtualThreadPerTaskExecutor` to handle thousands of concurrent pipelines with minimal footprint.
* **Adaptive Backpressure:** Internal semaphore-based flow control that pauses ingestion when downstream sinks are saturated.
* **SPI Plugin System:** Fully modular design. Source, Sink, and Transform logic are loaded dynamically at runtime using Java's Service Provider Interface (SPI).

### Enterprise Reliability
* **Schema Enforcement:** First-class support for **Avro** and **Schema Registry** to prevent "data trash" in downstream systems.
* **Resilience:** Configurable **Dead Letter Queues (DLQ)** and "Stop-the-World" safe shutdown mechanisms ensuring zero data loss during restarts.
* **Observability:** Built-in **Prometheus** metrics endpoint tracking throughput, latency, and cache hit rates.

### Modern Intelligence
* **AI-Ready:** Includes transformers for **Vector Embedding** generation (RAG support).
* **Polyglot Storage:** Drivers for **Kafka**, **MongoDB (Vector Search)**, and generic HTTP endpoints.

---

## Documentation

- ARCHITECTURE.md
- MODULES.md
- PROFILES.md
- PLAYBOOKS.md
- METRICS.md
- SECURITY.md

---

## Quick Start

docker compose up -d  
./gradlew clean build -x test  
java -jar build/libs/StreamKernel-*-all.jar

---

## 🗺 Roadmap

### Phase 1: Security Hardening (Completed 12/23/2025)
* [x] **mTLS Encryption:** Enforce strict mutual TLS between the Kernel and Kafka brokers.
* [x] **RBAC:** Integration with Open Policy Agent (OPA) for topic-level authorization.

### Phase 2: Cloud Native
* [ ] **Kubernetes Support:** Helm charts for deploying StreamKernel as a scalable `Deployment`.
* [ ] **Istio Integration:** Service Mesh sidecar injection for zero-trust networking.
* [ ] **Operator Pattern:** Custom Resource Definition (CRD) to manage pipelines via `kubectl apply -f pipeline.yaml`.

---

## Why Apache License 2.0?

StreamKernel is licensed under the Apache License 2.0 to strike a balance between
open collaboration and enterprise-grade safety.

Apache 2.0 was chosen because it:

- ✔ Allows free use, modification, and redistribution (including commercial use)
- ✔ Provides an explicit patent grant (important for distributed systems)
- ✔ Protects contributors and users from patent litigation
- ✔ Is widely accepted by enterprises and cloud providers
- ✔ Avoids viral copyleft obligations (unlike GPL)

This makes StreamKernel suitable for:
- Open-source experimentation
- Enterprise adoption
- Commercial derivatives and SaaS offerings
- Long-term ecosystem growth

Apache 2.0 ensures StreamKernel can evolve without limiting how others build on it.

---
## License

StreamKernel is licensed under the Apache License, Version 2.0.

Copyright © 2025 Steven Lopez.

This project is developed and maintained by Steven Lopez.