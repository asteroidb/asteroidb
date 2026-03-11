use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::api;
use asteroidb_sample_app::shared::types::CertInfo;

#[component]
pub fn CertBadge(
    #[prop(into)] task_id: String,
    certification: Option<CertInfo>,
) -> impl IntoView {
    let cert_status = RwSignal::new(
        certification
            .as_ref()
            .map(|c| c.status.clone())
            .unwrap_or_else(|| "Pending".to_string()),
    );

    // Poll certification status every 500ms if pending
    let task_id_poll = task_id.clone();
    let _ = Effect::new(move |_| {
        let current = cert_status.get();
        if current != "Pending" {
            return;
        }
        let tid = task_id_poll.clone();
        let handle = gloo_timers::callback::Interval::new(500, move || {
            let tid = tid.clone();
            spawn_local(async move {
                if let Ok(resp) = api::get_cert_status(&tid).await {
                    if let Some(status) = resp.get("status").and_then(|v| v.as_str()) {
                        cert_status.set(status.to_string());
                    }
                }
            });
        });
        on_cleanup(move || drop(handle));
    });

    let badge_class = Memo::new(move |_| {
        match cert_status.get().as_str() {
            "Certified" => "cert-badge certified",
            "Rejected" => "cert-badge rejected",
            "Timeout" => "cert-badge timeout",
            _ => "cert-badge pending",
        }
    });

    let badge_text = Memo::new(move |_| {
        match cert_status.get().as_str() {
            "Certified" => "Certified",
            "Rejected" => "Rejected",
            "Timeout" => "Timeout",
            _ => "Pending...",
        }
        .to_string()
    });

    view! {
        <span class=move || badge_class.get()>
            {move || badge_text.get()}
        </span>
    }
}
