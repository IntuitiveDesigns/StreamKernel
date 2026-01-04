# This script creates your README and CHANGELOG without using complex terminal commands
$readmePath = "$pwd\README.md"
$changelogPath = "$pwd\CHANGELOG.md"

$readmeContent = @"
# StreamKernel
[![Architected by Steven Lopez](https://img.shields.io/badge/Architected%20by-Steven%20Lopez-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/steve-lopez-b9941/)

---

![Java 21](https://img.shields.io/badge/Java-21-orange?style=for-the-badge&logo=java)
![Kafka](https://img.shields.io/badge/Kafka-3.6-black?style=for-the-badge&logo=apachekafka)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue?style=for-the-badge)](LICENSE)

![StreamKernel logo](assets/StreamKernel-logo.png)

**StreamKernel** is a high-performance, enterprise-grade event orchestration engine designed to bridge the gap between low-latency data movement and intelligent data enrichment.

Created and maintained by **Steven Lopez**, this framework utilizes **Java 21 Virtual Threads** and **ZGC** to serve as a lightweight, modular "kernel" for streaming data.

---

## 🚀 Key Features

### Core Architecture
* **Multi-Module Design**: Clean separation between \`core\`, \`spi\`, and \`plugins\`.
* **Virtual Thread Orchestrator**: Handles thousands of concurrent pipelines with minimal footprint.
* **Ring Buffer Source**: Capable of generating **>13 Million EPS** (events/sec) with zero steady-state garbage collection.

### Enterprise Reliability
* **Security First**: First-class support for **OPA (Open Policy Agent)** sidecars with fail-closed logic and caching.
* **Resilience**: Configurable **Dead Letter Queues (DLQ)** and safe shutdown mechanisms.

---

## 🏗 Architecture

StreamKernel follows a "Kernel + Plugin" architecture. The Kernel manages the thread lifecycle, while Plugins handle the I/O.

\`\`\`mermaid
graph TD
  subgraph SK["StreamKernel (Host JVM)"]
    SRC["Source Plugin"]
    ORCH["Pipeline Orchestrator"]
    AUTH["OPA Authorizer"]
    BP["Backpressure / Ring Buffer"]
    SINK["Sink Plugin"]
  end

  SRC --> ORCH
  ORCH --> AUTH
  AUTH -->|allow| BP --> SINK
  AUTH --> OPA["OPA Server"]
  SINK --> KAFKA["Kafka Broker"]
\`\`\`

## 📂 Project Structure

| Module | Description |
| :--- | :--- |
| \`streamkernel-spi\` | **Service Provider Interfaces**. The strict contract for plugins. |
| \`streamkernel-core\` | **The Engine**. Handles lifecycle, configuration, and orchestration. |
| \`streamkernel-plugins\` | **Extensions**. Contains synthetic sources, Kafka sinks, etc. |
| \`streamkernel-app\` | **Bootstrap**. The executable entry point for the application. |

---

## ⚡ Quick Start

### 1. Infrastructure Setup
\`\`\`bash
docker compose up -d
\`\`\`

### 2. OPA Sidecar (Manual Setup)
Run OPA in a separate window to watch authorization logs:
\`\`\`bash
docker run -p 8181:8181 openpolicyagent/opa run --server --log-level debug
\`\`\`

### 3. Build and Run
\`\`\`bash
./gradlew clean build -x test
./gradlew :streamkernel-app:run "-Dsk.config.path=config/profiles/01_synthetic-bench.properties"
\`\`\`

---

## 📜 License

StreamKernel is licensed under the **Apache License 2.0**.

Copyright © 2026 **IntuitiveDesigns**.
"@

$changelogContent = @"
# Changelog

All notable changes to the **StreamKernel** project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-01-03
### 🚀 Major Architecture Overhaul
This release marks the transition from a monolithic prototype to an Enterprise Multi-Module Architecture under **Apache License 2.0**.

### Added
- **Multi-Module Structure**: Separation of \`spi\`, \`core\`, \`plugins\`, and \`app\`.
- **High-Performance Source**: \`source-synthetic\` plugin with Lock-Free Ring Buffer (>13M EPS).
- **Enterprise Security**: \`security-opa\` plugin with Fail-Closed logic and caching.
- **Configuration**: Profile-based configuration via \`SK_CONFIG_PATH\`.

### Changed
- **License**: Switched from MIT to **Apache License 2.0**.
- **Identity**: Repository ownership transferred to **IntuitiveDesigns**.

## [0.0.1] - 2025-10-15
### Added
- Initial proof-of-concept prototype.
"@

Set-Content -Path $readmePath -Value $readmeContent -Encoding UTF8
Set-Content -Path $changelogPath -Value $changelogContent -Encoding UTF8

Write-Host "Done! README.md and CHANGELOG.md have been fixed." -ForegroundColor Green