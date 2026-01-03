# Changelog

All notable changes to the **StreamKernel** project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-01-03
### 🚀 Major Architecture Overhaul
This release marks the transition from a monolithic prototype to an Enterprise Multi-Module Architecture under **Apache License 2.0**.

### Added
- **Multi-Module Structure**: Split codebase into isolated modules:
    - \streamkernel-spi\ (Interfaces & Contracts)
    - \streamkernel-core\ (Engine & Orchestrator)
    - \streamkernel-plugins\ (Extensions)
    - \streamkernel-app\ (Bootstrap)
- **High-Performance Source**: Added \source-synthetic\ plugin implementing a **Lock-Free Ring Buffer**.
    - Verified throughput: **>13 Million EPS** per thread (Zero-Allocation payload reuse).
    - Deterministic SplitMix64 high-entropy generation.
- **Enterprise Security**: Added \security-opa\ plugin for Open Policy Agent integration.
    - Implemented **Fail-Closed** authorization logic.
    - Added short-lived caching with gated flush (DoS protection).
- **Configuration**:
    - Profile-based pipeline configuration via \SK_CONFIG_PATH\.
    - Explicit validation of required keys (\source.type\, \sink.type\).
    - Docker-first configuration model aligned with Kubernetes ConfigMaps.
- **Observability**: Added vendor-agnostic \MetricsRuntime\ SPI (Prometheus support).

### Changed
- **License**: Changed project license from MIT to **Apache License 2.0**.
- **Identity**: Repository ownership transferred to **IntuitiveDesigns**.
- **Orchestrator**: Re-engineered \PipelineOrchestrator\ to support **Virtual Thread per-task isolation**.
- **Startup**: StreamKernel now fails fast on misconfiguration instead of partially initializing.

### Removed
- **Monolith Artifacts**: Removed legacy \CustomerEvent\ and flat package structure.
- **Implicit Config**: Removed automatic fallback to \pipeline.properties\ (explicit profile required).

## [0.0.1] - 2025-10-15
### Added
- Initial proof-of-concept prototype.
- Basic Kafka Producer/Consumer loop.
- Virtual Thread Orchestrator proof-of-concept.
