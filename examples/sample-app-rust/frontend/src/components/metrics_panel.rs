use leptos::prelude::*;

use crate::state::use_app_state;

#[component]
pub fn MetricsPanel() -> impl IntoView {
    let state = use_app_state();

    let metrics = Memo::new(move |_| {
        state
            .metrics_json
            .get()
            .and_then(|json| serde_json::from_str::<serde_json::Value>(&json).ok())
    });

    view! {
        <div class="panel metrics-panel">
            <h2>"Metrics"</h2>
            {move || {
                match metrics.get() {
                    Some(m) => {
                        let pending = m.get("pending_count").and_then(|v| v.as_u64()).unwrap_or(0);
                        let certified = m.get("certified_total").and_then(|v| v.as_u64()).unwrap_or(0);
                        let cert_latency = m.get("certification_latency_mean_us").and_then(|v| v.as_f64()).unwrap_or(0.0);
                        let skew = m.get("frontier_skew_ms").and_then(|v| v.as_u64()).unwrap_or(0);
                        let sync_fail = m.get("sync_failure_rate").and_then(|v| v.as_f64()).unwrap_or(0.0);
                        let writes = m.get("write_ops_total").and_then(|v| v.as_u64()).unwrap_or(0);

                        view! {
                            <div class="metrics-grid">
                                <div class="metric-card">
                                    <div class="metric-value">{pending}</div>
                                    <div class="metric-label">"Pending Certifications"</div>
                                </div>
                                <div class="metric-card">
                                    <div class="metric-value">{certified}</div>
                                    <div class="metric-label">"Total Certified"</div>
                                </div>
                                <div class="metric-card">
                                    <div class="metric-value">{format!("{:.0}us", cert_latency)}</div>
                                    <div class="metric-label">"Cert Latency (mean)"</div>
                                </div>
                                <div class="metric-card">
                                    <div class="metric-value">{format!("{}ms", skew)}</div>
                                    <div class="metric-label">"Frontier Skew"</div>
                                </div>
                                <div class="metric-card">
                                    <div class="metric-value">{format!("{:.1}%", sync_fail * 100.0)}</div>
                                    <div class="metric-label">"Sync Failure Rate"</div>
                                </div>
                                <div class="metric-card">
                                    <div class="metric-value">{writes}</div>
                                    <div class="metric-label">"Total Writes"</div>
                                </div>
                            </div>
                        }.into_any()
                    }
                    None => {
                        view! { <p class="no-data">"Connecting to cluster..."</p> }.into_any()
                    }
                }
            }}
        </div>
    }
}
