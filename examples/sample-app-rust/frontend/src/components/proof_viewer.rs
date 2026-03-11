use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::api;

#[component]
pub fn ProofViewer(
    #[prop(into)] task_id: String,
) -> impl IntoView {
    let expanded = RwSignal::new(false);
    let proof_json = RwSignal::new(None::<String>);
    let verify_result = RwSignal::new(None::<String>);
    let verifying = RwSignal::new(false);

    let task_id_expand = task_id.clone();
    let task_id_verify = task_id.clone();

    let on_expand = move |_| {
        let is_expanded = expanded.get();
        expanded.set(!is_expanded);
        if !is_expanded && proof_json.get().is_none() {
            let tid = task_id_expand.clone();
            spawn_local(async move {
                if let Ok(resp) = api::get_cert_status(&tid).await {
                    let formatted = serde_json::to_string_pretty(&resp).unwrap_or_default();
                    proof_json.set(Some(formatted));
                }
            });
        }
    };

    let on_verify = move |_| {
        let tid = task_id_verify.clone();
        verifying.set(true);
        verify_result.set(None);
        spawn_local(async move {
            match api::verify_proof(&tid).await {
                Ok(result) => {
                    let text = if result.valid {
                        format!(
                            "Valid! Majority: {} ({}/{} authorities)",
                            result.has_majority,
                            result.contributing_count,
                            result.required_count
                        )
                    } else {
                        format!(
                            "Invalid. Majority: {} ({}/{} authorities)",
                            result.has_majority,
                            result.contributing_count,
                            result.required_count
                        )
                    };
                    verify_result.set(Some(text));
                }
                Err(e) => {
                    verify_result.set(Some(format!("Verification error: {e}")));
                }
            }
            verifying.set(false);
        });
    };

    view! {
        <div class="proof-viewer">
            <button class="btn btn-small" on:click=on_expand>
                {move || if expanded.get() { "Hide Proof" } else { "View Proof" }}
            </button>

            <Show when=move || expanded.get()>
                <div class="proof-content">
                    {move || proof_json.get().map(|json| {
                        view! { <pre class="proof-json">{json}</pre> }
                    })}

                    <button
                        class="btn btn-small btn-verify"
                        on:click=on_verify
                        disabled=move || verifying.get()
                    >
                        {move || if verifying.get() { "Verifying..." } else { "Verify Independently" }}
                    </button>

                    {move || verify_result.get().map(|result| {
                        let class = if result.starts_with("Valid") {
                            "verify-result valid"
                        } else {
                            "verify-result invalid"
                        };
                        view! { <div class=class>{result}</div> }
                    })}
                </div>
            </Show>
        </div>
    }
}
