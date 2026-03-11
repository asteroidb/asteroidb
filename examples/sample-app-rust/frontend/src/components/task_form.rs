use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::api;
use crate::state::use_app_state;

#[component]
pub fn TaskForm(
    #[prop(into)] on_close: Callback<()>,
) -> impl IntoView {
    let state = use_app_state();
    let title = RwSignal::new(String::new());
    let description = RwSignal::new(String::new());

    let on_submit = move |ev: leptos::ev::SubmitEvent| {
        ev.prevent_default();
        let t = title.get();
        let d = description.get();
        let tasks_signal = state.tasks;
        let close = on_close;
        spawn_local(async move {
            match api::create_task(&t, &d).await {
                Ok(_) => {
                    if let Ok(tasks) = api::fetch_tasks().await {
                        tasks_signal.set(tasks);
                    }
                    close.run(());
                }
                Err(e) => {
                    web_sys::window()
                        .and_then(|w| w.alert_with_message(&format!("Error: {e}")).ok());
                }
            }
        });
    };

    let on_cancel = move |_| {
        on_close.run(());
    };

    view! {
        <div class="modal-overlay">
            <div class="modal">
                <h2>"New Task"</h2>
                <form on:submit=on_submit>
                    <div class="form-group">
                        <label for="title">"Title"</label>
                        <input
                            id="title"
                            type="text"
                            required=true
                            prop:value=move || title.get()
                            on:input=move |ev| title.set(event_target_value(&ev))
                        />
                    </div>
                    <div class="form-group">
                        <label for="description">"Description"</label>
                        <textarea
                            id="description"
                            rows="3"
                            prop:value=move || description.get()
                            on:input=move |ev| description.set(event_target_value(&ev))
                        />
                    </div>
                    <div class="form-actions">
                        <button type="submit" class="btn btn-primary">"Create"</button>
                        <button type="button" class="btn" on:click=on_cancel>"Cancel"</button>
                    </div>
                </form>
            </div>
        </div>
    }
}
