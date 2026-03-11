use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::api;
use crate::state::use_app_state;

#[component]
pub fn VoteButton(
    #[prop(into)] task_id: String,
    votes: i64,
) -> impl IntoView {
    let state = use_app_state();
    let task_id_up = task_id.clone();
    let task_id_down = task_id.clone();

    let on_upvote = move |_| {
        let tid = task_id_up.clone();
        let tasks_signal = state.tasks;
        spawn_local(async move {
            let _ = api::vote_task(&tid, true).await;
            if let Ok(tasks) = api::fetch_tasks().await {
                tasks_signal.set(tasks);
            }
        });
    };

    let on_downvote = move |_| {
        let tid = task_id_down.clone();
        let tasks_signal = state.tasks;
        spawn_local(async move {
            let _ = api::vote_task(&tid, false).await;
            if let Ok(tasks) = api::fetch_tasks().await {
                tasks_signal.set(tasks);
            }
        });
    };

    view! {
        <div class="vote-button">
            <button class="btn-icon btn-vote-up" on:click=on_upvote title="Upvote">
                "+"
            </button>
            <span class="vote-count">{votes}</span>
            <button class="btn-icon btn-vote-down" on:click=on_downvote title="Downvote">
                "-"
            </button>
        </div>
    }
}
