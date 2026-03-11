use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::api;
use crate::components::metrics_panel::MetricsPanel;
use crate::components::slo_panel::SloPanel;
use crate::components::topology_map::TopologyMap;
use crate::state::use_app_state;

#[component]
pub fn DashboardPage() -> impl IntoView {
    let state = use_app_state();

    // Initial load
    Effect::new(move |_| {
        let metrics_sig = state.metrics_json;
        let slo_sig = state.slo_json;
        let topo_sig = state.topology_json;
        let health_sig = state.health_json;
        spawn_local(async move {
            load_dashboard(metrics_sig, slo_sig, topo_sig, health_sig).await;
        });
    });

    // Poll every 5 seconds
    let _ = Effect::new(move |_| {
        let metrics_sig = state.metrics_json;
        let slo_sig = state.slo_json;
        let topo_sig = state.topology_json;
        let health_sig = state.health_json;
        let handle = gloo_timers::callback::Interval::new(5_000, move || {
            spawn_local(async move {
                load_dashboard(metrics_sig, slo_sig, topo_sig, health_sig).await;
            });
        });
        on_cleanup(move || drop(handle));
    });

    view! {
        <div class="dashboard-page">
            <h1>"Cluster Dashboard"</h1>
            <div class="dashboard-grid">
                <MetricsPanel />
                <SloPanel />
                <TopologyMap />
            </div>
        </div>
    }
}

async fn load_dashboard(
    metrics_sig: RwSignal<Option<String>>,
    slo_sig: RwSignal<Option<String>>,
    topo_sig: RwSignal<Option<String>>,
    health_sig: RwSignal<Option<String>>,
) {
    if let Ok(m) = api::fetch_metrics().await {
        metrics_sig.set(Some(m));
    }
    if let Ok(s) = api::fetch_slo().await {
        slo_sig.set(Some(s));
    }
    if let Ok(t) = api::fetch_topology().await {
        topo_sig.set(Some(t));
    }
    if let Ok(h) = api::fetch_health().await {
        health_sig.set(Some(h));
    }
}
