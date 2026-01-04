# Architecture

StreamKernel follows a Kernel + Plugin architecture.

Source → Kernel → Transformer → Sink  
↓  
OPA Authorization  
↓  
Dead Letter Queue (DLQ)

Includes adaptive backpressure, retry routing, and observability hooks.
