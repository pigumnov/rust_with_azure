use tokio::time::{interval, Duration};
use tiberius::{Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::{TokioAsyncWriteCompatExt, Compat};
use std::fs;
use std::path::Path;

mod job;

fn load_env() -> Result<String, Box<dyn std::error::Error>> {
    let content = fs::read_to_string(Path::new(".env"))?;
    
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        
        if let Some((key, value)) = line.split_once('=') {
            if key.trim() == "DATABASE_URL" {
                return Ok(value.trim().to_string());
            }
        }
    }
    
    Err("DATABASE_URL not found in .env".into())
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    //dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();
    let database_url = load_env()?;
    println!("DATABASE_URL found: {}", database_url);
    // let database_url = env::var("DATABASE_URL")
    //     .expect("DATABASE_URL not set");
    
    println!("Automation job started...");

    let mut ticker = interval(Duration::from_secs(120));

    loop {
        ticker.tick().await;

        tracing::info!("Starting job iteration...");

        //Create a new client for any iteration
        match create_client(&database_url).await {
            Ok(mut client) => {
                if let Err(e) = job::run(&mut client).await {
                    tracing::error!("Job failed: {:?}", e);
                }
            },
            Err(e) => {
                tracing::error!("Failed to create database client: {:?}", e);
            }
        }
    }
}

async fn create_client(database_url: &str) -> Result<Client<Compat<TcpStream>>, Box<dyn std::error::Error>> {
    let config = Config::from_ado_string(database_url)?;
    
    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;
    
    //Use compat_write() to get Compat<TcpStream>
    let client = Client::connect(config, tcp.compat_write()).await?;
    
    Ok(client)
}