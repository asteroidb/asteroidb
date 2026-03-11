use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::api;
use crate::state::use_app_state;
use asteroidb_sample_app::shared::types::TagAction;

#[component]
pub fn TagEditor(
    #[prop(into)] task_id: String,
    tags: Vec<String>,
) -> impl IntoView {
    let state = use_app_state();
    let new_tag = RwSignal::new(String::new());
    let task_id_add = task_id.clone();

    let on_add_tag = move |ev: leptos::ev::KeyboardEvent| {
        if ev.key() == "Enter" {
            ev.prevent_default();
            let tag = new_tag.get();
            if tag.is_empty() {
                return;
            }
            let tid = task_id_add.clone();
            let tasks_signal = state.tasks;
            new_tag.set(String::new());
            spawn_local(async move {
                let _ = api::update_tags(&tid, TagAction::Add, &tag).await;
                if let Ok(tasks) = api::fetch_tasks().await {
                    tasks_signal.set(tasks);
                }
            });
        }
    };

    let tags_view: Vec<_> = tags
        .into_iter()
        .map(|tag| {
            let task_id = task_id.clone();
            let tag_remove = tag.clone();
            let tasks_signal = state.tasks;
            let on_remove = move |_| {
                let tid = task_id.clone();
                let t = tag_remove.clone();
                spawn_local(async move {
                    let _ = api::update_tags(&tid, TagAction::Remove, &t).await;
                    if let Ok(tasks) = api::fetch_tasks().await {
                        tasks_signal.set(tasks);
                    }
                });
            };
            view! {
                <span class="tag">
                    {&tag}
                    <button class="tag-remove" on:click=on_remove>"x"</button>
                </span>
            }
        })
        .collect();

    view! {
        <div class="tag-editor">
            <div class="tag-list">
                {tags_view}
            </div>
            <input
                class="tag-input"
                type="text"
                placeholder="Add tag..."
                prop:value=move || new_tag.get()
                on:input=move |ev| new_tag.set(event_target_value(&ev))
                on:keydown=on_add_tag
            />
        </div>
    }
}
