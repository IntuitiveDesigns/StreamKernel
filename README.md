# StreamKernel

StreamKernel is a high-performance, Kafka-native event orchestration engine designed to decouple **pipeline orchestration** from **payload semantics**.

It models real-world, production-grade streaming workloads including backpressure, enrichment, durability, security, and observability, while sustaining extremely high throughput using Java 21 Virtual Threads and ZGC.

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

## License

Apache License, Version 2.0
