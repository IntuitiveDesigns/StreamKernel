# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to Semantic Versioning.

---

## [4.0.0] – 2025-12-27

### Added
- Profile-based pipeline configuration via `SK_CONFIG_PATH`
- Support for environment-specific, immutable pipeline definitions
- Explicit validation of required pipeline keys:
    - `pipeline.name`
    - `source.type`
    - `sink.type`
- Improved startup diagnostics for missing or invalid configuration
- Docker-first configuration model aligned with Kubernetes ConfigMaps
- Prometheus metrics initialization confirmed under profile-based configs

### Changed
- StreamKernel now loads **exactly one pipeline definition at startup**
- Configuration precedence standardized:
    1. Environment variables (`SK_*`)
    2. System properties
    3. Profile file selected via `SK_CONFIG_PATH`
- Docker Compose now mirrors enterprise deployment behavior
- Startup fails fast on misconfiguration instead of partially initializing

### Removed
- Implicit loading of `pipeline.properties`
- Automatic fallback to classpath configuration
- Ambiguous multi-file configuration behavior

### Breaking Changes
- `pipeline.properties` is no longer used or supported
- A pipeline **must** define `source.type` explicitly
- Configuration profiles must be selected explicitly via `SK_CONFIG_PATH`
- Existing deployments relying on classpath defaults must migrate

### Migration Notes
To migrate from older versions:

1. Select an existing profile under `config/profiles/`
2. Ensure it defines:
    - `pipeline.name`
    - `source.type`
    - `sink.type`
3. Set the environment variable:
   ```bash
   SK_CONFIG_PATH=/etc/streamkernel/config/profiles/<profile>.properties
