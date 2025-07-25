use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use tokio::sync::RwLock;
use tonic::transport::Server;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt};
use vkcp::{controller, grpc::vkcp_server::VkcpServer};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "config.toml")]
    config: String,
    #[arg(short, long, default_value = "debug")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let cfg = vkcp::config::Config::new(&args.config)?;
    let current_master = Arc::new(RwLock::new(cfg.servers[0].clone()));
    let layer = tracing_subscriber::fmt::layer().compact();
    let filter = EnvFilter::builder().parse(format!("info,vkcp={}", args.log_level))?;
    let subscriber = tracing_subscriber::registry().with(layer).with(filter);
    tracing::subscriber::set_global_default(subscriber)?;

    // starting valkey proxy
    let proxy_addr = format!(
        "{}:{}",
        cfg.host.clone().unwrap_or(String::from("[::1]")),
        cfg.proxy_port
    );
    tokio::spawn(vkcp::proxy::start_proxy(proxy_addr, current_master.clone()));

    // starting controller
    let ctrl_addr = format!(
        "{}:{}",
        cfg.host.clone().unwrap_or(String::from("[::1]")),
        cfg.ctrl_port
    )
    .parse()?;
    let ctrl =
        controller::Server::new(&cfg, current_master.clone()).context("starting controller")?;
    Server::builder()
        .add_service(VkcpServer::new(ctrl))
        .serve(ctrl_addr)
        .await?;

    Ok(())
}
