use leptos::prelude::*;

use crate::state::use_app_state;

#[component]
pub fn SloPanel() -> impl IntoView {
    let state = use_app_state();

    let slo_data = Memo::new(move |_| {
        state
            .slo_json
            .get()
            .and_then(|json| serde_json::from_str::<serde_json::Value>(&json).ok())
    });

    view! {
        <div class="panel slo-panel">
            <h2>"SLO Budgets"</h2>
            {move || {
                match slo_data.get() {
                    Some(data) => {
                        let budgets = data
                            .get("budgets")
                            .and_then(|b| b.as_object())
                            .cloned()
                            .unwrap_or_default();

                        let mut budget_views: Vec<_> = budgets
                            .iter()
                            .map(|(name, budget)| {
                                let remaining = budget
                                    .get("budget_remaining")
                                    .and_then(|v| v.as_f64())
                                    .unwrap_or(100.0);
                                let is_warning = budget
                                    .get("is_warning")
                                    .and_then(|v| v.as_bool())
                                    .unwrap_or(false);
                                let is_critical = budget
                                    .get("is_critical")
                                    .and_then(|v| v.as_bool())
                                    .unwrap_or(false);
                                let total = budget
                                    .get("total_requests")
                                    .and_then(|v| v.as_u64())
                                    .unwrap_or(0);
                                let violations = budget
                                    .get("violations")
                                    .and_then(|v| v.as_u64())
                                    .unwrap_or(0);

                                let bar_class = if is_critical {
                                    "slo-bar-fill critical"
                                } else if is_warning {
                                    "slo-bar-fill warning"
                                } else {
                                    "slo-bar-fill healthy"
                                };

                                let display_name = name
                                    .replace('_', " ")
                                    .split(' ')
                                    .map(|w| {
                                        let mut c = w.chars();
                                        match c.next() {
                                            None => String::new(),
                                            Some(f) => f.to_uppercase().to_string() + c.as_str(),
                                        }
                                    })
                                    .collect::<Vec<_>>()
                                    .join(" ");

                                (name.clone(), view! {
                                    <div class="slo-item">
                                        <div class="slo-header">
                                            <span class="slo-name">{display_name}</span>
                                            <span class="slo-stats">
                                                {format!("{:.1}% remaining ({}/{})", remaining, violations, total)}
                                            </span>
                                        </div>
                                        <div class="slo-bar">
                                            <div
                                                class=bar_class
                                                style=format!("width: {}%", remaining.min(100.0))
                                            />
                                        </div>
                                    </div>
                                })
                            })
                            .collect();

                        budget_views.sort_by(|a, b| a.0.cmp(&b.0));
                        let views: Vec<_> = budget_views.into_iter().map(|(_, v)| v).collect();

                        view! { <div class="slo-list">{views}</div> }.into_any()
                    }
                    None => {
                        view! { <p class="no-data">"Connecting to cluster..."</p> }.into_any()
                    }
                }
            }}
        </div>
    }
}
