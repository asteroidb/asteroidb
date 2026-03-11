#[cfg(feature = "server")]
mod config;
mod error;
#[cfg(feature = "server")]
mod handlers;
#[cfg(feature = "server")]
mod proxy;
#[cfg(feature = "server")]
mod routes;
pub mod shared;

#[cfg(feature = "server")]
#[tokio::main]
async fn main() {
    use std::sync::Arc;

    use clap::Parser;
    use tower_http::cors::CorsLayer;
    use tower_http::services::ServeDir;
    use tracing_subscriber::EnvFilter;

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();

    let cfg = config::Config::parse();

    tracing::info!(
        port = cfg.port,
        nodes = ?cfg.nodes,
        static_dir = cfg.static_dir,
        "starting AsteroidDB sample app BFF"
    );

    let proxy = Arc::new(proxy::AsteroidProxy::new(cfg.nodes));

    let app = routes::api_router(proxy)
        .layer(CorsLayer::permissive())
        .fallback_service(ServeDir::new(&cfg.static_dir));

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", cfg.port))
        .await
        .expect("failed to bind");

    tracing::info!("listening on http://0.0.0.0:{}", cfg.port);

    axum::serve(listener, app).await.expect("server error");
}

#[cfg(not(feature = "server"))]
fn main() {
    // This crate is used as a library (shared types only) when "server" feature is disabled.
    panic!("This binary requires the 'server' feature. Run with: cargo run --features server");
}
