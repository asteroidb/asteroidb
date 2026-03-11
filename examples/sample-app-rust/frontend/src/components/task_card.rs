use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::api;
use crate::components::cert_badge::CertBadge;
use crate::components::proof_viewer::ProofViewer;
use crate::components::tag_editor::TagEditor;
use crate::components::vote_button::VoteButton;
use crate::state::use_app_state;
use asteroidb_sample_app::shared::types::{Task, TaskStatus};

#[component]
pub fn TaskCard(task: Task) -> impl IntoView {
    let state = use_app_state();
    let task_id = task.id.clone();
    let task_id_delete = task.id.clone();
    let task_id_status = task.id.clone();
    let is_done = task.status == TaskStatus::Done;

    let on_delete = move |_| {
        let tid = task_id_delete.clone();
        let tasks_signal = state.tasks;
        spawn_local(async move {
            let _ = api::delete_task(&tid).await;
            if let Ok(tasks) = api::fetch_tasks().await {
                tasks_signal.set(tasks);
            }
        });
    };

    let on_status_change = move |ev: leptos::ev::Event| {
        let tid = task_id_status.clone();
        let tasks_signal = state.tasks;
        let value = event_target_value(&ev);
        if let Ok(new_status) = value.parse::<TaskStatus>() {
            spawn_local(async move {
                let _ = api::update_status(&tid, new_status).await;
                if let Ok(tasks) = api::fetch_tasks().await {
                    tasks_signal.set(tasks);
                }
            });
        }
    };

    let status_str = task.status.to_string();

    view! {
        <div class="task-card">
            <div class="task-card-header">
                <h3 class="task-title">{&task.title}</h3>
                <button class="btn-icon btn-delete" on:click=on_delete title="Delete">
                    "x"
                </button>
            </div>

            {(!task.description.is_empty()).then(|| {
                view! { <p class="task-description">{&task.description}</p> }
            })}

            <div class="task-controls">
                <select
                    class="status-select"
                    on:change=on_status_change
                >
                    <option value="todo" selected=status_str == "todo">"Todo"</option>
                    <option value="doing" selected=status_str == "doing">"Doing"</option>
                    <option value="done" selected=status_str == "done">"Done"</option>
                </select>

                <VoteButton task_id=task_id.clone() votes=task.votes />
            </div>

            <TagEditor task_id=task_id.clone() tags=task.tags.clone() />

            {is_done.then(|| {
                let tid = task_id.clone();
                let cert = task.certification.clone();
                view! {
                    <div class="cert-section">
                        <CertBadge task_id=tid.clone() certification=cert />
                        <ProofViewer task_id=tid />
                    </div>
                }
            })}
        </div>
    }
}
