use anyhow::Context;
use clap::Parser;
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
    let layer = tracing_subscriber::fmt::layer().json().flatten_event(true);
    let filter = EnvFilter::builder().parse(format!("info,vkcp={}", args.log_level))?;
    let subscriber = tracing_subscriber::registry().with(layer).with(filter);
    tracing::subscriber::set_global_default(subscriber)?;

    let (ctrl, actions) = controller::Server::new(&cfg).context("starting controller")?;

    // starting valkey proxy
    tokio::spawn(vkcp::proxy::start_proxy(cfg.proxy_bind_addr, actions));

    // starting controller
    Server::builder()
        .add_service(VkcpServer::new(ctrl))
        .serve(
            cfg.ctrl_bind_addr
                .parse()
                .context("parsing controller bind address")?,
        )
        .await?;

    Ok(())
}
