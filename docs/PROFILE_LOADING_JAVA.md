# Optional: Best-practice profile loading (Java)

This is a drop-in approach that keeps performance high and avoids adding dependencies.

## Resolution order (recommended)

1. `-Dstreamkernel.profile=<name>`
2. `--profile=<name>`
3. `STREAMKERNEL_PROFILE=<name>`
4. fallback: `config/application.properties` (or `config/default.properties`)

## Suggested `PipelineConfig` API

- `PipelineConfig.load(profileName)` loads:
  - `config/default.properties` (optional baseline)
  - `config/profiles/<profileName>.properties` (override)
  - then applies `-Dkey=value` overrides if you want runtime switches

## Suggested `KafkaApp` entry

Parse `--profile` from args, pass into `PipelineConfig.load(profile)`.
