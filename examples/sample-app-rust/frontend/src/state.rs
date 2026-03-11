use leptos::prelude::*;

use asteroidb_sample_app::shared::types::Task;

/// Global application state provided via Leptos context.
#[derive(Clone)]
pub struct AppState {
    pub tasks: RwSignal<Vec<Task>>,
    pub selected_task: RwSignal<Option<String>>,
    pub metrics_json: RwSignal<Option<String>>,
    pub slo_json: RwSignal<Option<String>>,
    pub topology_json: RwSignal<Option<String>>,
    pub health_json: RwSignal<Option<String>>,
    pub loading: RwSignal<bool>,
    pub error: RwSignal<Option<String>>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            tasks: RwSignal::new(Vec::new()),
            selected_task: RwSignal::new(None),
            metrics_json: RwSignal::new(None),
            slo_json: RwSignal::new(None),
            topology_json: RwSignal::new(None),
            health_json: RwSignal::new(None),
            loading: RwSignal::new(false),
            error: RwSignal::new(None),
        }
    }
}

/// Provide the global AppState to the component tree.
pub fn provide_app_state() {
    let state = AppState::new();
    provide_context(state);
}

/// Use the global AppState from the component tree.
pub fn use_app_state() -> AppState {
    expect_context::<AppState>()
}
