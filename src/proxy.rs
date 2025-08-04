use anyhow::{Context, Result};
use metrics::{counter, gauge};
use tokio::{
    net::{TcpListener, TcpStream, ToSocketAddrs},
    select,
    sync::{mpsc::Sender, oneshot},
};
use tracing::{debug, error, warn};

use crate::state_machine;

const METRIC_PROXY_CONNECTIONS: &str = "vkcp.proxy.connections";
const METRIC_PROXY_REQUESTS: &str = "vkcp.proxy.requests";
const METRIC_PROXY_ERRORS: &str = "vkcp.proxy.errors";
const METRIC_PROXY_DENIED: &str = "vkcp.proxy.denied";

pub async fn start_proxy(
    addr: impl ToSocketAddrs,
    actions: Sender<state_machine::Message>,
) -> Result<()> {
    let listener = TcpListener::bind(addr).await.context("starting listener")?;
    while let Ok((ingress, _)) = listener.accept().await {
        counter!(METRIC_PROXY_REQUESTS).increment(1);
        tokio::spawn(proxy_connection(ingress, actions.clone()));
    }

    Ok(())
}

async fn proxy_connection(mut ingress: TcpStream, actions: Sender<state_machine::Message>) {
    let (tx, rx) = oneshot::channel();
    if let Err(e) = actions
        .send(state_machine::Message::GetCurrentMaster(tx))
        .await
    {
        warn!(error = format!("{e:#}"), "querying current master");
        return;
    }
    match rx.await {
        Ok(Some((addr, context))) => {
            let mut egress = match TcpStream::connect(addr).await {
                Ok(e) => e,
                Err(e) => {
                    counter!(METRIC_PROXY_ERRORS).increment(1);
                    error!(error = format!("{e:#}"), "connecting to valkey");
                    return;
                }
            };
            gauge!(METRIC_PROXY_CONNECTIONS).increment(1);
            select! {
                _ = context.cancelled() => {
                    // master was demoted and/or became unhealthy
                    debug!("terminating connection earlier");
                    return;
                }
                r = tokio::io::copy_bidirectional(&mut ingress, &mut egress) => {
                    match r {
                        Ok((to_egress, to_ingress)) => {
                            debug!("proxied {to_egress} bytes from client and {to_ingress} bytes from server");
                        }
                        Err(e) => {
                            counter!(METRIC_PROXY_ERRORS).increment(1);
                            warn!("error proxying: {e}");
                        }
                    }
                }
            }
            gauge!(METRIC_PROXY_CONNECTIONS).decrement(1);
        }
        Ok(None) => {
            counter!(METRIC_PROXY_DENIED).increment(1);
            debug!("proxying is disabled");
            return;
        }
        Err(e) => {
            warn!(error = format!("{e:#}"), "getting current master");
            return;
        }
    }
}
