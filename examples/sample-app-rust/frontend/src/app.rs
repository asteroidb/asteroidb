use leptos::prelude::*;
use leptos_router::components::{Route, Router, Routes};
use leptos_router::path;

use crate::components::board::BoardPage;
use crate::components::dashboard::DashboardPage;
use crate::state::provide_app_state;

#[component]
pub fn App() -> impl IntoView {
    provide_app_state();

    view! {
        <Router>
            <nav class="main-nav">
                <a href="/" class="nav-link">"Board"</a>
                <a href="/dashboard" class="nav-link">"Dashboard"</a>
                <span class="nav-title">"AsteroidDB Task Board"</span>
            </nav>
            <main>
                <Routes fallback=|| view! { <p>"Page not found"</p> }>
                    <Route path=path!("/") view=BoardPage />
                    <Route path=path!("/dashboard") view=DashboardPage />
                </Routes>
            </main>
        </Router>
    }
}
