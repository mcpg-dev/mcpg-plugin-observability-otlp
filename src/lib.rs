//! `dev.mcpg.observability.otlp` — OpenTelemetry OTLP-gRPC sink for
//! gateway telemetry.
//!
//! Implements [`SyncTelemetrySink`] against `opentelemetry-otlp`
//! 0.28 with the `grpc-tonic` exporter for span export. Logs and
//! metrics receive a tracing `debug` event for now (full OTLP-logs /
//! OTLP-metrics export is the dedicated `metrics_sink` and `log_sink`
//! plugins' responsibility — see `dev.mcpg.observability.prometheus`;
//! an OTLP-metrics + OTLP-logs companion plugin is not yet built).
//!
//! # Lifecycle
//!
//! 1. `from_config_json` parses the operator's `config:` block but
//!    does NOT yet install the SDK exporter. OTel installation is
//!    deferred to first emit because the FFI host might call
//!    `make()` from a non-tokio thread; we lazily fish the ambient
//!    runtime from [`tokio::runtime::Handle::try_current()`] on
//!    first emit (which always arrives from a tokio task via the
//!    telemetry-sink bridge).
//! 2. `span_started` stashes the [`SpanStart`] event in a pending
//!    map keyed by `(trace_id, span_id)`. Span construction defers
//!    until the matching `span_ended` arrives.
//! 3. `span_ended` pops the matching start, builds an OTel
//!    span via the lazy-initialized tracer, and ends it. The
//!    SDK's batch span processor handles the actual OTLP push.
//! 4. `flush` calls [`SdkTracerProvider::force_flush`] to drain
//!    the batch queue.
//! 5. `shutdown` calls [`SdkTracerProvider::shutdown`] to stop the
//!    batch worker cleanly.

use std::collections::BTreeMap;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mcpg_plugin_protocol::{
    PluginClass, PluginManifest,
    logs::LogRecord,
    telemetry::{MetricPoint, SpanEnd, SpanStart, SpanStatus, TelemetryError},
};
use mcpg_plugin_sdk::declare_plugin;
use mcpg_plugin_sdk::ffi::SyncTelemetrySink;
use opentelemetry::{
    KeyValue,
    trace::{Span, SpanKind as OtelSpanKind, Status as OtelStatus, Tracer, TracerProvider},
};
use opentelemetry_sdk::{Resource, trace::SdkTracerProvider};
use parking_lot::Mutex;
use serde::Deserialize;

/// Plugin id — operators reference this in
/// `observability.traces.sinks[].kind`.
pub const PLUGIN_ID: &str = "dev.mcpg.observability.otlp";

/// Operator config schema.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct OtlpSinkConfig {
    /// OTLP gRPC endpoint, e.g. `http://otel-collector:4317`. The
    /// plugin runs in degraded mode (events dropped + tracing
    /// warn) when this is empty so observability never blocks
    /// gateway boot.
    pub url: String,
    /// `service.name` resource attribute. Defaults to `"mcpg"`.
    pub service_name: String,
    /// Optional `service.version` resource attribute.
    pub service_version: Option<String>,
    /// Free-form additional resource attributes (e.g.
    /// `deployment.environment: prod`).
    pub resource_attributes: BTreeMap<String, String>,
    /// Per-export timeout for the OTLP gRPC call.
    pub batch_export_timeout_ms: u64,
}

impl Default for OtlpSinkConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            service_name: "mcpg".into(),
            service_version: None,
            resource_attributes: BTreeMap::new(),
            batch_export_timeout_ms: 30_000,
        }
    }
}

/// Pending span starts awaiting their matching `span_ended`. Keyed
/// by `(trace_id, span_id)` strings as they appear on the wire.
type PendingSpans = BTreeMap<(String, String), SpanStart>;

pub struct OtlpSink {
    manifest: PluginManifest,
    config: OtlpSinkConfig,
    /// SDK tracer provider — lazily initialized on first emit.
    /// Wrapped in `OnceLock` to enforce single-init.
    provider: OnceLock<SdkTracerProvider>,
    /// Pending starts keyed by `(trace_id, span_id)`.
    pending: Mutex<PendingSpans>,
}

impl OtlpSink {
    pub fn from_config_json(config_json: &str) -> Self {
        // Fail CLOSED: a present-but-malformed operator `config:` block
        // refuses the plugin (panic → null handle / boot rejection)
        // rather than silently degrading to defaults. An empty / absent
        // block still yields `OtlpSinkConfig::default()`.
        let config: OtlpSinkConfig =
            mcpg_plugin_sdk::fail_closed_config!(config_json, OtlpSinkConfig);
        Self::with_config(config)
    }

    pub fn with_config(config: OtlpSinkConfig) -> Self {
        Self {
            manifest: PluginManifest {
                id: PLUGIN_ID.into(),
                version: env!("CARGO_PKG_VERSION").into(),
                name: "OpenTelemetry OTLP Exporter".into(),
                plugin_class: PluginClass::TelemetrySink,
                protocol_version: "1.0".into(),
                license: None,
                required_capabilities: Vec::new(),
                tags: Vec::new(),
                provides: Vec::new(),
                provides_schemes: Vec::new(),
                module_path_prefix: ::std::module_path!()
                    .split("::")
                    .next()
                    .unwrap_or("")
                    .to_owned(),
                backend_profile: None,
            },
            config,
            provider: OnceLock::new(),
            pending: Mutex::new(BTreeMap::new()),
        }
    }

    /// Lazily initialize the SDK tracer provider on first emit.
    /// Returns `None` (degraded mode) when the configured URL is
    /// empty or no tokio runtime is available.
    fn ensure_provider(&self) -> Option<&SdkTracerProvider> {
        if let Some(p) = self.provider.get() {
            return Some(p);
        }
        if self.config.url.is_empty() {
            tracing::warn!(
                plugin_id = PLUGIN_ID,
                "OTLP sink missing required `url` config — span dropped"
            );
            return None;
        }
        if tokio::runtime::Handle::try_current().is_err() {
            tracing::warn!(
                plugin_id = PLUGIN_ID,
                "no tokio runtime available — OTLP sink in degraded mode (events dropped)"
            );
            return None;
        }
        let provider = match build_provider(&self.config) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(
                    plugin_id = PLUGIN_ID,
                    error = %e,
                    "failed to build OTLP tracer provider — span dropped"
                );
                return None;
            }
        };
        let _ = self.provider.set(provider);
        self.provider.get()
    }

    #[cfg(test)]
    fn pending_count(&self) -> usize {
        self.pending.lock().len()
    }
}

fn build_provider(config: &OtlpSinkConfig) -> Result<SdkTracerProvider, String> {
    use opentelemetry_otlp::{SpanExporter, WithExportConfig};

    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(config.url.clone())
        .with_timeout(Duration::from_millis(config.batch_export_timeout_ms))
        .build()
        .map_err(|e| format!("OTLP exporter build failed: {e}"))?;

    let mut resource_kvs: Vec<KeyValue> = Vec::with_capacity(2 + config.resource_attributes.len());
    resource_kvs.push(KeyValue::new("service.name", config.service_name.clone()));
    if let Some(v) = &config.service_version {
        resource_kvs.push(KeyValue::new("service.version", v.clone()));
    }
    for (k, v) in &config.resource_attributes {
        resource_kvs.push(KeyValue::new(k.clone(), v.clone()));
    }
    let resource = Resource::builder().with_attributes(resource_kvs).build();

    Ok(SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build())
}

fn ns_to_systemtime(ns: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_nanos(ns)
}

fn convert_status(status: &SpanStatus) -> OtelStatus {
    match status {
        SpanStatus::Ok => OtelStatus::Ok,
        SpanStatus::Error { message } => OtelStatus::error(message.clone()),
        SpanStatus::Unset => OtelStatus::Unset,
    }
}

fn serde_json_to_otel_value(v: &serde_json::Value) -> opentelemetry::Value {
    match v {
        serde_json::Value::Null => opentelemetry::Value::String("".into()),
        serde_json::Value::Bool(b) => opentelemetry::Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                opentelemetry::Value::I64(i)
            } else if let Some(f) = n.as_f64() {
                opentelemetry::Value::F64(f)
            } else {
                opentelemetry::Value::String(n.to_string().into())
            }
        }
        serde_json::Value::String(s) => opentelemetry::Value::String(s.clone().into()),
        // Nested arrays / objects: encode as JSON for the operator
        // backend to round-trip via downstream parsing.
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            opentelemetry::Value::String(v.to_string().into())
        }
    }
}

impl SyncTelemetrySink for OtlpSink {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn span_started(&self, span: &SpanStart) {
        // Eagerly init on first event so the operator notices
        // misconfig early. Discard return — provider may still be
        // None, in which case we skip on span_ended.
        let _ = self.ensure_provider();
        let key = (span.trace_id.clone(), span.span_id.clone());
        self.pending.lock().insert(key, span.clone());
    }

    fn span_ended(&self, span: &SpanEnd) {
        use mcpg_plugin_protocol::telemetry::SpanKind as ProtoKind;

        let key = (span.trace_id.clone(), span.span_id.clone());
        let start = match self.pending.lock().remove(&key) {
            Some(s) => s,
            None => {
                tracing::debug!(
                    plugin_id = PLUGIN_ID,
                    trace_id = %span.trace_id,
                    span_id = %span.span_id,
                    "span_ended without matching span_started — dropping"
                );
                // Outcome counter so operators can tell whether spans
                // are being dropped because of a bug.
                metrics::counter!(
                    "mcpg_otlp_spans_total",
                    "outcome" => "dropped_orphan",
                )
                .increment(1);
                return;
            }
        };
        let provider = match self.ensure_provider() {
            Some(p) => p,
            None => {
                metrics::counter!(
                    "mcpg_otlp_spans_total",
                    "outcome" => "dropped_no_provider",
                )
                .increment(1);
                return;
            }
        };

        let tracer = provider.tracer("mcpg-plugin-observability-otlp");
        let kind = match start.kind {
            ProtoKind::Internal => OtelSpanKind::Internal,
            ProtoKind::Server => OtelSpanKind::Server,
            ProtoKind::Client => OtelSpanKind::Client,
            ProtoKind::Producer => OtelSpanKind::Producer,
            ProtoKind::Consumer => OtelSpanKind::Consumer,
        };

        let mut attrs =
            Vec::with_capacity(start.attributes.len() + span.additional_attributes.len());
        for (k, v) in &start.attributes {
            attrs.push(KeyValue::new(k.clone(), serde_json_to_otel_value(v)));
        }
        for (k, v) in &span.additional_attributes {
            attrs.push(KeyValue::new(k.clone(), serde_json_to_otel_value(v)));
        }

        let mut builder = tracer
            .span_builder(start.name.clone())
            .with_start_time(ns_to_systemtime(start.start_ns))
            .with_kind(kind);
        if !attrs.is_empty() {
            builder = builder.with_attributes(attrs);
        }

        let mut otel_span = tracer.build(builder);
        otel_span.set_status(convert_status(&span.status));
        otel_span.end_with_timestamp(ns_to_systemtime(span.end_ns));
        metrics::counter!(
            "mcpg_otlp_spans_total",
            "outcome" => "exported",
        )
        .increment(1);
    }

    fn metric_recorded(&self, metric: &MetricPoint) {
        // Metrics export is the dedicated `metrics_sink` entity
        // kind's responsibility (see `dev.mcpg.observability.
        // prometheus`). For operators who wire both this OTLP plugin
        // and a metrics sink, no action is needed here. A dedicated
        // OTLP-metrics MetricsSink companion plugin is not yet built.
        tracing::debug!(
            plugin_id = PLUGIN_ID,
            metric = %metric.name,
            "metric_recorded received — OTLP-metrics export deferred to Phase 6c-22"
        );
    }

    fn log_recorded(&self, record: &LogRecord) {
        // Same deferral as metric_recorded — operators wire a
        // dedicated `log_sink` plugin (Datadog / Honeycomb /
        // Splunk client) for log forwarding. This stub keeps the
        // trait contract intact + makes the deferral observable.
        tracing::debug!(
            plugin_id = PLUGIN_ID,
            target = %record.target,
            level = %record.level,
            "log_recorded received — OTLP-logs export deferred to Phase 6c-22"
        );
    }

    fn flush(&self, timeout_ms: u64) -> Result<(), TelemetryError> {
        let provider = match self.provider.get() {
            Some(p) => p,
            None => {
                metrics::counter!(
                    "mcpg_otlp_flushes_total",
                    "outcome" => "no_provider",
                )
                .increment(1);
                return Ok(());
            }
        };
        let started = std::time::Instant::now();
        // `SdkTracerProvider::force_flush` is a synchronous, potentially
        // unbounded blocking call (the OTel SDK exposes no timeout param),
        // so a stuck exporter could block the gateway's shutdown/flush path
        // indefinitely. Honour the caller's deadline: run the flush on a
        // detached worker and bound the wait with `recv_timeout`. On
        // overrun return `Timeout` and let the worker keep draining in the
        // background. `timeout_ms == 0` means "no deadline" — block until
        // fully drained (used by callers that want a complete flush).
        let result = if timeout_ms == 0 {
            provider.force_flush()
        } else {
            let provider = provider.clone();
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let _ = tx.send(provider.force_flush());
            });
            match rx.recv_timeout(std::time::Duration::from_millis(timeout_ms)) {
                Ok(r) => r,
                Err(_) => {
                    let elapsed_ms = started.elapsed().as_millis() as f64;
                    metrics::histogram!("mcpg_otlp_flush_duration_ms").record(elapsed_ms);
                    metrics::counter!(
                        "mcpg_otlp_flushes_total",
                        "outcome" => "timeout",
                    )
                    .increment(1);
                    return Err(TelemetryError::Timeout);
                }
            }
        };
        let elapsed_ms = started.elapsed().as_millis() as f64;
        metrics::histogram!("mcpg_otlp_flush_duration_ms").record(elapsed_ms);
        match result {
            Ok(()) => {
                metrics::counter!(
                    "mcpg_otlp_flushes_total",
                    "outcome" => "success",
                )
                .increment(1);
                Ok(())
            }
            Err(e) => {
                metrics::counter!(
                    "mcpg_otlp_flushes_total",
                    "outcome" => "error",
                )
                .increment(1);
                Err(TelemetryError::Backend {
                    reason: format!("force_flush failed: {e:?}"),
                })
            }
        }
    }

    fn shutdown(&self) {
        if let Some(p) = self.provider.get()
            && let Err(e) = p.shutdown()
        {
            tracing::warn!(
                plugin_id = PLUGIN_ID,
                error = ?e,
                "OTel tracer provider shutdown returned error"
            );
        }
    }
}

declare_plugin! {
    plugin_id: PLUGIN_ID,
    plugin_version: env!("CARGO_PKG_VERSION"),
    descriptor_yaml: include_str!("../plugin.yaml"),
    capabilities: &[mcpg_plugin_protocol::capability::Capability::NetworkOutbound],
    entities: [
        telemetry_sink as entity {
            inner_name: "",
            plugin_type: OtlpSink,
            factory: |cfg, _host: ::mcpg_plugin_sdk::HostHandle| OtlpSink::from_config_json(cfg),
        }
    ],
}

// ---------------------------------------------------------------------------
// Async trait bridge
// ---------------------------------------------------------------------------
//
// The macro wires the cdylib FFI surface against the SDK's
// `SyncTelemetrySink`. For first-party static linking (the gateway
// registers the plugin via [`FirstPartyRegistrar`]), we also need
// the async [`mcpg_plugin_protocol::telemetry::TelemetrySink`]
// impl. Both surfaces forward to the same internal state.

#[mcpg_plugin_protocol::async_trait]
impl mcpg_plugin_protocol::telemetry::TelemetrySink for OtlpSink {
    fn manifest(&self) -> &PluginManifest {
        <Self as SyncTelemetrySink>::manifest(self)
    }

    async fn span_started(&self, span: SpanStart) {
        <Self as SyncTelemetrySink>::span_started(self, &span);
    }

    async fn span_ended(&self, span: SpanEnd) {
        <Self as SyncTelemetrySink>::span_ended(self, &span);
    }

    async fn metric_recorded(&self, metric: MetricPoint) {
        <Self as SyncTelemetrySink>::metric_recorded(self, &metric);
    }

    async fn log_recorded(&self, record: &LogRecord) {
        <Self as SyncTelemetrySink>::log_recorded(self, record);
    }

    async fn flush(&self, timeout: std::time::Duration) -> Result<(), TelemetryError> {
        let timeout_ms = timeout.as_millis().min(u64::MAX as u128) as u64;
        <Self as SyncTelemetrySink>::flush(self, timeout_ms)
    }

    async fn shutdown(&self) {
        <Self as SyncTelemetrySink>::shutdown(self);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use mcpg_plugin_protocol::telemetry::{SpanKind as ProtoKind, SpanStatus};

    fn config_with_url() -> OtlpSinkConfig {
        OtlpSinkConfig {
            url: "http://127.0.0.1:4317".into(),
            service_name: "mcpg-test".into(),
            ..Default::default()
        }
    }

    fn span_start(trace_id: &str, span_id: &str) -> SpanStart {
        SpanStart {
            trace_id: trace_id.into(),
            span_id: span_id.into(),
            parent_id: None,
            name: "test.span".into(),
            kind: ProtoKind::Internal,
            start_ns: 1_000_000,
            attributes: BTreeMap::new(),
        }
    }

    fn span_end(trace_id: &str, span_id: &str) -> SpanEnd {
        SpanEnd {
            trace_id: trace_id.into(),
            span_id: span_id.into(),
            end_ns: 2_000_000,
            status: SpanStatus::Ok,
            events: Vec::new(),
            additional_attributes: BTreeMap::new(),
        }
    }

    #[test]
    fn manifest_carries_telemetry_sink_class_and_capability() {
        // Capabilities live on
        // PluginRegistration.capabilities (typed). Manifest's
        // Vec<String> is display-only.
        let sink = OtlpSink::with_config(config_with_url());
        assert_eq!(sink.manifest().id, PLUGIN_ID);
        assert_eq!(sink.manifest().plugin_class, PluginClass::TelemetrySink);
    }

    #[test]
    fn config_default_has_mcpg_service_name() {
        let cfg: OtlpSinkConfig = Default::default();
        assert_eq!(cfg.service_name, "mcpg");
        assert_eq!(cfg.batch_export_timeout_ms, 30_000);
    }

    #[test]
    fn config_rejects_unknown_fields() {
        let res =
            serde_json::from_str::<OtlpSinkConfig>(r#"{"url": "x", "totally_unknown_field": 1}"#);
        assert!(res.is_err(), "deny_unknown_fields should reject typos");
    }

    #[test]
    #[should_panic(expected = "failing closed")]
    fn from_config_json_fails_closed_on_malformed() {
        // A present-but-malformed operator `config:` block must REFUSE
        // the plugin (fail closed) rather than silently degrade to
        // defaults — the SDK helper panics, which the FFI `make` slot
        // turns into a boot rejection.
        let _ = OtlpSink::from_config_json("not json");
    }

    #[test]
    fn from_config_json_empty_block_yields_defaults() {
        // An empty / absent / unit config block is an opt-out, not a
        // typo, so it still produces the default config.
        for empty in ["", "   ", "{}", "null"] {
            let sink = OtlpSink::from_config_json(empty);
            assert!(sink.config.url.is_empty());
            assert_eq!(sink.config.service_name, "mcpg");
            assert_eq!(sink.config.batch_export_timeout_ms, 30_000);
        }
    }

    #[test]
    fn pending_map_is_empty_after_construction() {
        let sink = OtlpSink::with_config(config_with_url());
        assert_eq!(sink.pending_count(), 0);
    }

    #[test]
    fn span_started_stashes_into_pending_map() {
        // No tokio runtime active -> ensure_provider returns None,
        // but span_started still records the pending start.
        let sink = OtlpSink::with_config(config_with_url());
        sink.span_started(&span_start("a", "b"));
        assert_eq!(sink.pending_count(), 1);
    }

    #[test]
    fn span_ended_without_matching_start_drops_silently() {
        let sink = OtlpSink::with_config(config_with_url());
        // No runtime -> provider is None -> early-return path.
        sink.span_ended(&span_end("never", "started"));
        assert_eq!(sink.pending_count(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn span_started_then_ended_clears_pending() {
        // Real tokio runtime; ensure_provider succeeds at the
        // exporter-build step (tonic builder doesn't connect at
        // build time, so an unreachable URL is acceptable).
        let sink = OtlpSink::with_config(config_with_url());
        sink.span_started(&span_start("trace-1", "span-1"));
        assert_eq!(sink.pending_count(), 1);
        sink.span_ended(&span_end("trace-1", "span-1"));
        assert_eq!(sink.pending_count(), 0);
        // shutdown is idempotent + best-effort.
        sink.shutdown();
    }

    #[test]
    fn flush_returns_ok_when_provider_uninitialized() {
        let sink = OtlpSink::with_config(config_with_url());
        assert!(sink.flush(1000).is_ok());
    }

    #[test]
    fn descriptor_yaml_id_matches_plugin_id_const() {
        assert!(
            DESCRIPTOR_YAML.contains(&format!("id: {PLUGIN_ID}")),
            "descriptor YAML id should match PLUGIN_ID const"
        );
        assert!(
            DESCRIPTOR_YAML.contains("class: telemetry_sink"),
            "descriptor YAML class should be telemetry_sink"
        );
        assert!(
            DESCRIPTOR_YAML.contains("network_outbound"),
            "descriptor YAML must declare outbound_network capability"
        );
    }

    #[test]
    fn serde_json_value_to_otel_handles_primitives() {
        assert!(matches!(
            serde_json_to_otel_value(&serde_json::json!(true)),
            opentelemetry::Value::Bool(true)
        ));
        assert!(matches!(
            serde_json_to_otel_value(&serde_json::json!(42i64)),
            opentelemetry::Value::I64(42)
        ));
        assert!(matches!(
            serde_json_to_otel_value(&serde_json::json!(2.5)),
            opentelemetry::Value::F64(_)
        ));
        match serde_json_to_otel_value(&serde_json::json!("hello")) {
            opentelemetry::Value::String(s) => assert_eq!(s.as_ref(), "hello"),
            _ => panic!("expected string"),
        }
    }

    #[test]
    fn convert_status_round_trips_variants() {
        assert!(matches!(convert_status(&SpanStatus::Ok), OtelStatus::Ok));
        assert!(matches!(
            convert_status(&SpanStatus::Unset),
            OtelStatus::Unset
        ));
        match convert_status(&SpanStatus::Error {
            message: "oops".into(),
        }) {
            OtelStatus::Error { description } => assert_eq!(description.as_ref(), "oops"),
            other => panic!("expected error, got {other:?}"),
        }
    }
}
