#[cfg(feature = "server")]
use clap::Parser;

/// BFF server configuration.
#[cfg(feature = "server")]
#[derive(Debug, Parser)]
#[command(name = "asteroidb-sample-app", about = "AsteroidDB Sample Task Board")]
pub struct Config {
    /// Port to listen on.
    #[arg(long, env = "BFF_PORT", default_value = "8080")]
    pub port: u16,

    /// Comma-separated list of AsteroidDB node URLs.
    #[arg(
        long,
        env = "ASTEROIDB_NODES",
        value_delimiter = ',',
        default_value = "http://localhost:3001"
    )]
    pub nodes: Vec<String>,

    /// Path to the frontend dist directory (trunk build output).
    #[arg(long, env = "STATIC_DIR", default_value = "frontend/dist")]
    pub static_dir: String,
}
