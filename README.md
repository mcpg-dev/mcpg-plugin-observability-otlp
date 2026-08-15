# OpenTelemetry OTLP Exporter — `dev.mcpg.observability.otlp`

> class `telemetry_sink` · `native` · package `mcpg-plugin-observability-otlp` · artifact `libmcpg_plugin_observability_otlp.so` · Apache-2.0

Ships MCP gateway traces to an OpenTelemetry Collector — or to any
OTLP-compatible backend such as Datadog, Honeycomb, Grafana Tempo, or New Relic
— over OTLP/gRPC. The plugin consumes the gateway's span lifecycle events,
reassembles each start/end pair into an OpenTelemetry span with its attributes
and status, and hands it to the OTel SDK's batch span processor. Because the
processor owns delivery, ending a span is a fire-and-forget enqueue and a slow
collector never back-pressures the request path. Reach for it when you want
gateway request traces in the same distributed-tracing view as the services
behind it.

This plugin exports **traces only**. Metric and log events that reach it are
recorded as a `debug` line and otherwise ignored; wire a `metrics_sink` and a
`log_sink` for those signals.

## What it does
- Buffers each `span_started` event keyed by `(trace_id, span_id)` and builds
  the OpenTelemetry span when the matching `span_ended` arrives, carrying over
  name, kind, start and end timestamps, status, and both attribute sets.
- Maps span kinds one-for-one onto the OTel vocabulary (`internal`, `server`,
  `client`, `producer`, `consumer`).
- Converts attribute values by JSON type; arrays and objects are encoded as
  JSON strings so nothing is silently dropped.
- Stamps every span with `service.name`, optionally `service.version`, and any
  resource attributes you declare.
- Declares the `network_outbound` capability, consumed by the gRPC exporter.
- Degrades instead of failing: the exporter is built lazily on the first span,
  and an empty `url`, an absent Tokio runtime, or a failed exporter build logs a
  warning and drops spans so observability never blocks gateway boot.
- Honours the caller's flush deadline — a `flush` with a non-zero timeout runs
  the SDK drain on a worker and returns a timeout error rather than blocking
  shutdown indefinitely.

## Configuration
Referenced by id from the dedicated `observability.traces.sinks[]` list — not
from the `plugins:` list. The plugin is compiled into the gateway binary and
registers itself when its id appears in that list and the traces signal is on.

Capability grants are read separately, from the `plugins:` entry whose `id` is
the plugin id: whatever that entry lists under `granted_capabilities` is what
the plugin is granted. Because this plugin declares `network_outbound`, that
grant has to be present, or registration fails and the gateway refuses to start
with a capability error. Config validation applies to such an entry like any
other — it must name exactly one of `source.path` or `source.oci`.

```yaml
observability:
  enabled: true
  traces:
    enabled: true
    service_name: mcpg
    propagate_context: true
    sinks:
      - kind: dev.mcpg.observability.otlp
        config:
          url: http://otel-collector:4317
          service_name: mcpg
          service_version: "2.4.0"
          resource_attributes:
            deployment.environment: prod
          batch_export_timeout_ms: 30000
```

| Field | Type | Default | Description |
|---|---|---|---|
| `url` | string | `""` | OTLP/gRPC endpoint. Empty puts the sink in degraded mode: spans are dropped and a warning is logged. |
| `service_name` | string | `mcpg` | `service.name` resource attribute on every exported span. |
| `service_version` | string or null | `null` | `service.version` resource attribute; omitted when unset. |
| `resource_attributes` | map<string,string> | `{}` | Additional resource attributes applied to every span. |
| `batch_export_timeout_ms` | integer | `30000` | Per-export timeout for the OTLP gRPC call. |

Unknown fields are rejected. A `config:` block that is present but does not
parse refuses the plugin at boot rather than silently reverting to defaults; an
absent or empty block yields the defaults above.

## Observability
Every span outcome is counted on `mcpg_otlp_spans_total`, labelled `outcome`:
`exported`, `dropped_orphan` (an end event with no matching start), or
`dropped_no_provider` (degraded mode). Flushes are counted on
`mcpg_otlp_flushes_total` with `outcome` of `success`, `error`, `timeout`, or
`no_provider`, and timed by the `mcpg_otlp_flush_duration_ms` histogram. A
rising `dropped_no_provider` means the endpoint is unset or unreachable at
provider-build time; a rising `timeout` means the collector is not draining as
fast as the gateway flushes.

## Build
Default feature set is OFF (avoids `mcpg_plugin_register` linker
collisions in the workspace build); opt in to the cdylib:

```bash
cargo build -p mcpg-plugin-observability-otlp --features cdylib-export --release   # → target/release/libmcpg_plugin_observability_otlp.so
```

## Sign & load (production)
Sign the artifact, pin/verify via the entry's `signature:` block, and honour
revocations. See <https://mcpg.dev/docs/security/plugin-security>.

## See also
- Observability signals and how sinks fan out: <https://mcpg.dev/docs/reference/configuration>
- Plugin classes and the loading contract: <https://mcpg.dev/docs/plugins/plugins-and-protocol>
- The metrics signal's canonical sink: `libs/plugins/observability/prometheus`
- The logs signal over syslog: `libs/plugins/observability/syslog`
