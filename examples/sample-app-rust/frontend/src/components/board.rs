use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::api;
use crate::components::column::Column;
use crate::components::task_form::TaskForm;
use crate::state::use_app_state;
use asteroidb_sample_app::shared::types::TaskStatus;

#[component]
pub fn BoardPage() -> impl IntoView {
    let state = use_app_state();
    let show_form = RwSignal::new(false);

    // Initial load and polling
    let tasks_signal = state.tasks;
    let error_signal = state.error;

    // Fetch tasks on mount and every 2 seconds
    Effect::new(move |_| {
        spawn_local(async move {
            load_tasks(tasks_signal, error_signal).await;
        });
    });

    // Set up polling interval
    let _ = Effect::new(move |_| {
        let handle = gloo_timers::callback::Interval::new(2_000, move || {
            spawn_local(async move {
                load_tasks(tasks_signal, error_signal).await;
            });
        });
        // Keep the interval alive by storing it
        on_cleanup(move || drop(handle));
    });

    view! {
        <div class="board-page">
            <div class="board-header">
                <h1>"Task Board"</h1>
                <button
                    class="btn btn-primary"
                    on:click=move |_| show_form.set(true)
                >
                    "+ New Task"
                </button>
            </div>

            {move || {
                error_signal.get().map(|err| {
                    view! {
                        <div class="error-banner">
                            {err}
                        </div>
                    }
                })
            }}

            <Show when=move || show_form.get()>
                <TaskForm on_close=move || show_form.set(false) />
            </Show>

            <div class="board">
                <Column title="Todo" status=TaskStatus::Todo />
                <Column title="Doing" status=TaskStatus::Doing />
                <Column title="Done" status=TaskStatus::Done />
            </div>
        </div>
    }
}

async fn load_tasks(
    tasks_signal: RwSignal<Vec<asteroidb_sample_app::shared::types::Task>>,
    error_signal: RwSignal<Option<String>>,
) {
    match api::fetch_tasks().await {
        Ok(tasks) => {
            tasks_signal.set(tasks);
            error_signal.set(None);
        }
        Err(e) => {
            error_signal.set(Some(format!("Failed to load tasks: {e}")));
        }
    }
}
