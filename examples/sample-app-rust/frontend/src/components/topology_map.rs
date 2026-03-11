use leptos::prelude::*;

use crate::state::use_app_state;
use asteroidb_sample_app::shared::types::{NodeHealth, TopologyInfo};

#[component]
pub fn TopologyMap() -> impl IntoView {
    let state = use_app_state();

    let topology = Memo::new(move |_| {
        state
            .topology_json
            .get()
            .and_then(|json| serde_json::from_str::<TopologyInfo>(&json).ok())
    });

    let health = Memo::new(move |_| {
        state
            .health_json
            .get()
            .and_then(|json| serde_json::from_str::<Vec<NodeHealth>>(&json).ok())
            .unwrap_or_default()
    });

    view! {
        <div class="panel topology-panel">
            <h2>"Cluster Topology"</h2>
            {move || {
                match topology.get() {
                    Some(topo) => {
                        let health_list = health.get();
                        let total = topo.total_nodes;

                        let region_views: Vec<_> = topo.regions.iter().map(|region| {
                            let node_views: Vec<_> = region.node_ids.iter().map(|nid| {
                                let is_healthy = health_list
                                    .iter()
                                    .find(|h| h.address.contains(nid))
                                    .map(|h| h.healthy)
                                    .unwrap_or(true);
                                let dot_class = if is_healthy {
                                    "node-dot healthy"
                                } else {
                                    "node-dot unhealthy"
                                };
                                view! {
                                    <span class=dot_class title=nid.clone()></span>
                                }
                            }).collect();

                            let latencies: Vec<_> = region.inter_region_latency_ms
                                .iter()
                                .map(|(target, ms)| {
                                    view! {
                                        <span class="latency-tag">
                                            {format!("-> {}: {:.0}ms", target, ms)}
                                        </span>
                                    }
                                })
                                .collect();

                            view! {
                                <div class="region-card">
                                    <div class="region-header">
                                        <strong>{&region.name}</strong>
                                        <span class="region-count">
                                            {format!("{} nodes", region.node_count)}
                                        </span>
                                    </div>
                                    <div class="region-nodes">{node_views}</div>
                                    {(!latencies.is_empty()).then(|| {
                                        view! { <div class="region-latencies">{latencies}</div> }
                                    })}
                                </div>
                            }
                        }).collect();

                        view! {
                            <div class="topology-info">
                                <p class="total-nodes">{format!("Total nodes: {}", total)}</p>
                                <div class="regions-grid">{region_views}</div>
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
