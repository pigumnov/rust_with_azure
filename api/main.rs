//pigumnov small api to use in one server with own application
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use std::env;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tiberius::{Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

mod job;


#[derive(Clone)]
struct AppState {
    database_url: Arc<String>,
}

#[derive(Serialize)]
struct ApiResponse {
    status: &'static str,
    message: String,
}

fn parse_database_url_from_env_file(path: &Path) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
    if !path.exists() {
        return Ok(None);
    }

    let content = fs::read_to_string(path)?;
    for raw_line in content.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if let Some((key, value)) = line.split_once('=') {
            if key.trim() == "DATABASE_URL" {
                let parsed = value.trim().to_string();
                if !parsed.is_empty() {
                    return Ok(Some(parsed));
                }
            }
        }
    }

    Ok(None)
}

fn load_env() -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    if let Ok(from_env) = env::var("DATABASE_URL") {
        let trimmed = from_env.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
    }

    if let Some(from_root_env) = parse_database_url_from_env_file(Path::new(".env"))? {
        return Ok(from_root_env);
    }

    if let Some(from_src_env) = parse_database_url_from_env_file(Path::new("src/.env"))? {
        return Ok(from_src_env);
    }

    Err("DATABASE_URL is not set (checked env var, .env and src/.env)".into())
}

async fn run_jobs_once(database_url: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut client = create_client(database_url).await?;
    job::run(&mut client).await?;

    Ok(())
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn run_now(State(state): State<AppState>) -> impl IntoResponse {
    tracing::info!("Manual run requested via API");
    match run_jobs_once(&state.database_url).await {
        Ok(_) => (
            StatusCode::OK,
            Json(ApiResponse {
                status: "ok",
                message: "Jobs completed successfully".to_string(),
            }),
        )
            .into_response(),
        Err(error) => {
            tracing::error!("Manual run failed: {:?}", error);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse {
                    status: "error",
                    message: format!("Jobs failed: {error}"),
                }),
            )
                .into_response()
        }
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();
    let database_url = load_env()?;
    tracing::info!("DATABASE_URL loaded");

    let state = AppState {
        database_url: Arc::new(database_url),
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/run", get(run_now))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    tracing::info!("API server started at http://0.0.0.0:8080");

    axum::serve(listener, app).await?;
    Ok(())
}

async fn create_client(
    database_url: &str,
) -> Result<Client<Compat<TcpStream>>, Box<dyn std::error::Error + Send + Sync>> {
    let config = Config::from_ado_string(database_url)?;

    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    let client = Client::connect(config, tcp.compat_write()).await?;

    Ok(client)
}
