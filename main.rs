use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use axum::{
    extract::Query,
};
use uuid::Uuid;
use serde::{Serialize, Deserialize};
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


#[derive(Deserialize)]
pub struct GetCustomersQuery {
    #[serde(alias = "owner_uuid")]
    pub owner_id: String,  
    pub page:i32,
    pub per_page:i32,
    pub sort_field: String,
    pub sort_asc: String,
    pub search: String,
    //GetData(int page, int perPage, string sortField, bool sortAsc, string search)
}


#[derive(Deserialize)]
pub struct GetSuppliersQuery {
    #[serde(alias = "owner_uuid")]
    pub owner_id: String,
    pub page: i32,
    pub per_page: i32,
    pub sort_field: String,
    pub sort_asc: String,
    pub search: String,
    pub get_potential_sups: Option<String>,
    pub get_blacklist_sups: Option<String>,
}



//////GetData CustomerService
#[derive(Serialize)]
struct CustomerIndexDataItem {
    Id: Uuid,
    Name: String,
}

#[derive(Serialize)]
struct CustomerIndexData {
    Total: i64,
    Data: Vec<CustomerIndexDataItem>,
}

async fn customers_once(database_url: &str, 
    owner_id:String, 
    page:i32,
    per_page:i32, 
    sort_field:String,
    sort_asc:bool,
    search:String) -> Result<CustomerIndexData, Box<dyn std::error::Error + Send + Sync>> {
    let mut client = create_client(database_url).await?;

    // parse owner id to Uuid
    let owner_uuid = Uuid::parse_str(owner_id.trim())?;

    let (total, customers) = job::get_data_customers(
        &mut client,
        owner_uuid,
        page,
        per_page,
        sort_field,
        sort_asc,
        search,
    )
    .await?;

    let items = customers
        .into_iter()
        .map(|c| CustomerIndexDataItem { Id: c.id, Name: c.name })
        .collect();

    Ok(CustomerIndexData { Total: total, Data: items })
}

async fn customers_all(State(state): State<AppState>, 
Query(params): Query<GetCustomersQuery>,) -> impl IntoResponse {
    tracing::info!("Customers requested via API");
        // tolerant parse of sort_asc (accept True/true/1/yes/on)
        let sort_asc_flag = match params.sort_asc.to_lowercase().as_str() {
           "true" | "1" | "yes" | "on" => true,
           _ => false,
        };

        match customers_once(&state.database_url,
            params.owner_id,
            params.page,
            params.per_page,
            params.sort_field,
            sort_asc_flag,
            params.search
        ).await {
        Ok(data) => (
            StatusCode::OK,
            Json(data),
        )
            .into_response(),
        Err(error) => {
            tracing::error!("Customers request failed: {:?}", error);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse {
                    status: "error",
                    message: format!("Customers request failed: {error}"),
                }),
            )
                .into_response()
        }
    }
}
////////////// Suppliers

#[derive(Serialize)]
struct SupplierIndexDataItemOut {
    Id: Uuid,
    Name: String,
    PublicId: i32,
    StatusName: String,
    SupplierType: String,
    Emails: String,
}

#[derive(Serialize)]
struct SupplierIndexData {
    Total: i64,
    Data: Vec<SupplierIndexDataItemOut>,
}

async fn suppliers_once(database_url: &str,
    owner_id: String,
    page: i32,
    per_page: i32,
    sort_field: String,
    sort_asc: bool,
    search: String,
    get_potential_sups: bool,
    get_blacklist_sups: bool,
) -> Result<SupplierIndexData, Box<dyn std::error::Error + Send + Sync>> {
    let mut client = create_client(database_url).await?;

    let owner_uuid = Uuid::parse_str(owner_id.trim())?;

    let (total, items) = job::get_data_suppliers(&mut client,
        owner_uuid,
        page,
        per_page,
        sort_field,
        sort_asc,
        search,
        get_potential_sups,
        get_blacklist_sups,
    ).await?;

    let out_items = items.into_iter().map(|i| SupplierIndexDataItemOut {
        Id: i.id,
        Name: i.name,
        PublicId: i.public_id,
        StatusName: i.status_name,
        SupplierType: i.supplier_type,
        Emails: i.emails,
    }).collect();

    Ok(SupplierIndexData { Total: total, Data: out_items })
}

async fn suppliers_all(State(state): State<AppState>, Query(params): Query<GetSuppliersQuery>,) -> impl IntoResponse {
    tracing::info!("Suppliers requested via API");

    let sort_asc_flag = match params.sort_asc.to_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => true,
        _ => false,
    };

    let get_pot = match params.get_potential_sups.as_deref() {
        Some(v) => matches!(v.to_lowercase().as_str(), "true" | "1" | "yes" | "on"),
        None => false,
    };
    let get_black = match params.get_blacklist_sups.as_deref() {
        Some(v) => matches!(v.to_lowercase().as_str(), "true" | "1" | "yes" | "on"),
        None => false,
    };

    match suppliers_once(&state.database_url,
        params.owner_id,
        params.page,
        params.per_page,
        params.sort_field,
        sort_asc_flag,
        params.search,
        get_pot,
        get_black,
    ).await {
        Ok(data) => (
            StatusCode::OK,
            Json(data),
        ).into_response(),
        Err(error) => {
            tracing::error!("Suppliers request failed: {:?}", error);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse {
                    status: "error",
                    message: format!("Suppliers request failed: {error}"),
                }),
            ).into_response()
        }
    }
}
//////////////
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
        .route("/customers", get(customers_all))
        .route("/suppliers", get(suppliers_all))
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
