# Distributed Tracing

- [Distributed Tracing](#distributed-tracing)
  - [Overview](#overview)
  - [Architecture](#architecture)
  - [Docker Deployment](#docker-deployment)
  - [Kubernetes Deployment](#kubernetes-deployment)
  - [Configuration Reference](#configuration-reference)
  - [Local Development with Jaeger](#local-development-with-jaeger)
  - [Production Deployment](#production-deployment)

## Overview

OpenHouse uses the [OpenTelemetry Java Agent](https://opentelemetry.io/docs/zero-code/java/agent/) for
distributed tracing. The agent provides zero-code auto-instrumentation by attaching to the JVM at startup
via `-javaagent` and rewriting bytecode at class-load time using ByteBuddy.

The agent automatically instruments:

- **Spring MVC**: Controller entry/exit, request routing
- **Servlet filters**: Authentication, authorization interceptors
- **HTTP clients** (OkHttp, Reactor Netty): Cross-service calls with W3C TraceContext propagation
- **JDBC**: Full SQL statements, connection pool metrics
- **Spring Data JPA**: Repository method calls (`findById`, `save`, `deleteById`)
- **Hibernate**: Session operations, transaction commits

Each API request produces a **trace** — a tree of **spans** correlated by a single trace ID across
service boundaries. For example, a `POST /v1/databases/{db}/tables` (create table) generates ~62 spans
across the tables and housetables services, showing every cross-service HTTP call, JPA repository method,
and SQL statement.

## Architecture

OpenHouse services form a two-tier architecture for API calls:

```
Client (curl, Iceberg catalog)
  │
  ▼
┌─────────────────────────┐
│    Tables Service        │  ← receives REST API calls
│    (port 8080)           │
└────────┬────────────────┘
         │  HTTP (traced automatically)
         ▼
┌─────────────────────────┐
│  HouseTables Service     │  ← internal metadata store
│    (port 8080)           │
└────────┬────────────────┘
         │  JDBC (traced automatically)
         ▼
┌─────────────────────────┐
│       MySQL / DB         │
└─────────────────────────┘
```

The OTel agent injects `traceparent` and `tracestate` headers on outbound HTTP calls, so spans from
tables and housetables are automatically correlated into a single distributed trace.

### Coexistence with Metrics SDK

OpenHouse already uses the OpenTelemetry SDK for metrics (`DefaultOtelConfig.java`). When the Java Agent
is attached, it registers its own global `OpenTelemetry` instance that handles both tracing and metrics.
`DefaultOtelConfig` detects this via `GlobalOpenTelemetry.get()` and reuses the agent's instance instead
of creating a second SDK, which would cause an `IllegalStateException`.

When the agent is **not** attached (e.g., during unit tests or non-instrumented deployments),
`DefaultOtelConfig` falls back to creating a metrics-only SDK as before.

## Docker Deployment

### Image Build

The OTel agent JAR is downloaded during the Gradle build and copied into each Docker image:

```bash
# Download the agent JAR (run as part of `dockerPrereqs`)
./gradlew downloadOtelAgent
# Output: build/otel/opentelemetry-javaagent.jar (~21MB)
```

Each Dockerfile copies the agent into an `otel/` subdirectory. This placement is intentional — the
Dockerfiles contain a `find ./ -name "*.jar" -exec mv {} $APP_NAME.jar` step that renames all JARs
in the working directory. The agent must be in a subdirectory to avoid being caught by this rename.

```dockerfile
# After the JAR rename step and run.sh copy:
COPY build/otel/opentelemetry-javaagent.jar otel/opentelemetry-javaagent.jar
```

### Runtime Attachment

`run.sh` conditionally attaches the agent at JVM startup:

```bash
OTEL_AGENT_JAR="./otel/opentelemetry-javaagent.jar"
OTEL_AGENT_OPTS=""
if [ -f "$OTEL_AGENT_JAR" ] && [ "${OTEL_TRACES_ENABLED:-true}" != "false" ]; then
    OTEL_AGENT_OPTS="-javaagent:${OTEL_AGENT_JAR}"
fi

java $OTEL_AGENT_OPTS -Xmx256M -Xms64M -jar "$@"
```

This is backward compatible — if the agent JAR doesn't exist, the service starts without tracing.

## Kubernetes Deployment

In Kubernetes, the Java process is typically started directly from the Helm chart's `command:` field
rather than through `run.sh`. To enable tracing, add the `-javaagent` flag to the command:

```yaml
command:
  - "sh"
  - "-c"
  - >-
    java -javaagent:/home/openhouse/otel/opentelemetry-javaagent.jar
    -Xmx12288M -Xms12288M
    -XX:NativeMemoryTracking=summary
    -jar $APP_NAME.jar $@
```

### Agent JAR Delivery

The agent JAR can be included in the Docker image (recommended) or delivered via an init container.
When included in the image, ensure the `COPY` in the Dockerfile places it at the path referenced
in the `command:`.

### Environment Variables

Configure the agent via standard `OTEL_*` environment variables in the pod spec:

```yaml
env:
  - name: OTEL_SERVICE_NAME
    value: "openhouse-tables"
  - name: OTEL_EXPORTER_OTLP_ENDPOINT
    value: "http://otel-collector:4318"
  - name: OTEL_EXPORTER_OTLP_PROTOCOL
    value: "http/protobuf"
  - name: OTEL_TRACES_EXPORTER
    value: "otlp"
  - name: OTEL_METRICS_EXPORTER
    value: "none"           # Keep existing Prometheus metrics
  - name: OTEL_LOGS_EXPORTER
    value: "none"
  - name: OTEL_TRACES_SAMPLER
    value: "parentbased_traceidratio"
  - name: OTEL_TRACES_SAMPLER_ARG
    value: "0.01"           # 1% sampling for production
```

## Configuration Reference

All configuration is via environment variables (no code changes required):

| Variable | Description | Default |
|----------|-------------|---------|
| `OTEL_TRACES_ENABLED` | Set to `false` to disable agent attachment in `run.sh` | `true` |
| `OTEL_SERVICE_NAME` | Service name shown in traces | Auto-detected from JAR |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP collector endpoint | `http://localhost:4318` |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | OTLP protocol (`http/protobuf` or `grpc`) | `http/protobuf` |
| `OTEL_TRACES_EXPORTER` | Trace exporter (`otlp`, `none`, `logging`) | `otlp` |
| `OTEL_METRICS_EXPORTER` | Metric exporter (`none` to preserve Prometheus) | `otlp` |
| `OTEL_LOGS_EXPORTER` | Log exporter | `otlp` |
| `OTEL_TRACES_SAMPLER` | Sampling strategy | `parentbased_always_on` |
| `OTEL_TRACES_SAMPLER_ARG` | Sampler parameter (e.g., ratio for `traceidratio`) | `1.0` |
| `OTEL_RESOURCE_ATTRIBUTES` | Additional resource attributes (`key=value,key=value`) | — |

See the [OTel Java Agent configuration docs](https://opentelemetry.io/docs/zero-code/java/agent/configuration/)
for the full list.

## Local Development with Jaeger

The `oh-hadoop-spark` docker-compose recipe includes a [Jaeger](https://www.jaegertracing.io/)
all-in-one instance for local trace visualization:

```bash
# Start the full stack
./gradlew dockerUp -Precipe=oh-hadoop-spark

# Jaeger UI
open http://localhost:16686
```

Select `openhouse-tables` or `openhouse-housetables` in the Jaeger UI to view traces. Each trace
shows a waterfall/timeline view of all spans, which can be used to:

- **Build flamegraphs**: Identify where time is spent in an API call
- **Trace cross-service calls**: See tables → housetables → MySQL call chains
- **Identify redundant calls**: A create-table request makes ~6 existence-check round-trips
  before the actual write
- **View SQL statements**: Full queries are captured in JDBC spans

### Example Traces

| Operation | HTTP Status | Typical Spans | Services |
|-----------|-------------|---------------|----------|
| `GET /v1/databases` | 200 | ~6 | tables, housetables |
| `GET /v1/databases/{db}/tables/{tbl}` | 200 | ~16 | tables, housetables |
| `POST /v1/databases/{db}/tables` | 201 | ~62 | tables, housetables |
| `DELETE /v1/databases/{db}/tables/{tbl}` | 204 | ~50 | tables, housetables |

### Disabling Tracing Locally

To run without the agent in docker-compose, set `OTEL_TRACES_ENABLED=false`:

```yaml
environment:
  - OTEL_TRACES_ENABLED=false
```

## Production Deployment

### Sampling

For production, use `parentbased_traceidratio` with a low ratio to limit overhead:

```yaml
OTEL_TRACES_SAMPLER: parentbased_traceidratio
OTEL_TRACES_SAMPLER_ARG: "0.01"   # Sample 1% of traces
```

The `parentbased_` prefix ensures that if an incoming request already has a sampling decision
(from an upstream service), it is respected. Only root spans are subject to the ratio.

### Backend

The agent exports traces via OTLP to any compatible backend:

- **Jaeger** (local dev): Accepts OTLP directly on port 4317 (gRPC) / 4318 (HTTP)
- **OpenTelemetry Collector**: Recommended for production; can fan out to multiple backends
- **Grafana Tempo**, **Zipkin**, **Datadog**, etc.: Via the OTel Collector's exporter pipeline

### Resource Overhead

The agent adds approximately:
- **~50MB heap** for the agent itself and its instrumentation metadata
- **Negligible CPU** for span creation (nanosecond-level per span)
- **Network**: Depends on sampling rate and span volume; at 1% sampling the overhead is minimal

For production JVMs with multi-GB heaps, this overhead is insignificant.
