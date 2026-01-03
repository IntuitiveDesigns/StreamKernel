# Changelog

All notable changes to the **StreamKernel** project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-01-03
### 🚀 Major Architecture Overhaul
This release marks the transition from a monolithic prototype to an Enterprise Multi-Module Architecture under **Apache License 2.0**.

### Added
- **Multi-Module Structure**: Separation of \spi\, \core\, \plugins\, and \pp\.
- **High-Performance Source**: \source-synthetic\ plugin with Lock-Free Ring Buffer (>13M EPS).
- **Enterprise Security**: \security-opa\ plugin with Fail-Closed logic and caching.
- **Configuration**: Profile-based configuration via \SK_CONFIG_PATH\.

### Changed
- **License**: Switched from MIT to **Apache License 2.0**.
- **Identity**: Repository ownership transferred to **IntuitiveDesigns**.

## [0.0.1] - 2025-10-15
### Added
- Initial proof-of-concept prototype.
