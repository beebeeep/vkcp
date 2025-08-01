use anyhow::{Context, Result};
use tokio::{
    net::{TcpListener, TcpStream, ToSocketAddrs},
    select,
    sync::{mpsc::Sender, oneshot},
};
use tracing::{debug, error, warn};

use crate::state_machine;

pub async fn start_proxy(
    addr: impl ToSocketAddrs,
    actions: Sender<state_machine::Message>,
) -> Result<()> {
    let listener = TcpListener::bind(addr).await.context("starting listener")?;
    while let Ok((ingress, _)) = listener.accept().await {
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
                    error!(error = format!("{e:#}"), "connecting to valkey");
                    return;
                }
            };
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
                            warn!("error proxying: {e}");
                        }
                    }
                }
            }
        }
        Ok(None) => {
            debug!("proxying is disabled");
            return;
        }
        Err(e) => {
            warn!(error = format!("{e:#}"), "getting current master");
            return;
        }
    }
}
