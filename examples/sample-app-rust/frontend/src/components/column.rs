use leptos::prelude::*;

use crate::components::task_card::TaskCard;
use crate::state::use_app_state;
use asteroidb_sample_app::shared::types::TaskStatus;

#[component]
pub fn Column(
    #[prop(into)] title: String,
    status: TaskStatus,
) -> impl IntoView {
    let state = use_app_state();
    let title_clone = title.clone();

    let filtered_tasks = Memo::new(move |_| {
        state
            .tasks
            .get()
            .into_iter()
            .filter(|t| t.status == status)
            .collect::<Vec<_>>()
    });

    view! {
        <div class="column">
            <div class="column-header">
                <h2>{title_clone}</h2>
                <span class="task-count">{move || filtered_tasks.get().len()}</span>
            </div>
            <div class="column-body">
                <For
                    each=move || filtered_tasks.get()
                    key=|task| task.id.clone()
                    let:task
                >
                    <TaskCard task=task />
                </For>
            </div>
        </div>
    }
}
