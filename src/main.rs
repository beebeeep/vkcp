use std::net::{Ipv4Addr, SocketAddrV4};

use anyhow::Context;
use clap::Parser;
use metrics_exporter_prometheus::PrometheusBuilder;
use tonic::transport::Server;
use tracing::error;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt};
use vkcp::{controller, grpc::vkcp_server::VkcpServer};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "config.toml")]
    config: String,
    #[arg(short, long, default_value = "debug")]
    log_level: String,
    #[arg(long, default_value = "9090")]
    prometheus_port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let cfg = vkcp::config::Config::new(&args.config)?;
    let layer = tracing_subscriber::fmt::layer().json().flatten_event(true);
    let filter = EnvFilter::builder().parse(format!("info,vkcp={}", args.log_level))?;
    let subscriber = tracing_subscriber::registry().with(layer).with(filter);
    tracing::subscriber::set_global_default(subscriber)?;
    // TODO: start state machine separately
    let (ctrl, actions) = controller::Server::new(&cfg).context("starting controller")?;

    let mut tasks = Vec::new();
    let tags: Vec<(String, String)> = cfg.extra_tags.map_or(vec![], |v| v.into_iter().collect());

    // starting valkey proxy
    tasks.push(tokio::spawn(vkcp::proxy::start_proxy(
        cfg.proxy_bind_addr,
        actions,
        tags.clone(),
    )));

    // starting controller
    let ctrl_bind_addr = cfg
        .ctrl_bind_addr
        .parse()
        .context("parsing controller bind address")?;
    tasks.push(tokio::spawn(async move {
        let srv = Server::builder().add_service(VkcpServer::new(ctrl));
        srv.serve(ctrl_bind_addr)
            .await
            .context("serving controller")
    }));

    // starting metrics
    let builder = PrometheusBuilder::new();
    builder
        .with_http_listener(SocketAddrV4::new(
            Ipv4Addr::new(0, 0, 0, 0),
            args.prometheus_port,
        ))
        .install()
        .context("starting metrics")?;

    for task in tasks {
        if let Err(e) = task.await {
            error!(error = format!("{e:#}"), "task failed");
        }
    }

    Ok(())
}
