use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::{
    net::{TcpListener, TcpStream, ToSocketAddrs},
    sync::RwLock,
};

pub async fn start_proxy(addr: impl ToSocketAddrs, master_addr: Arc<RwLock<String>>) -> Result<()> {
    let listener = TcpListener::bind(addr).await.context("starting listener")?;
    while let Ok((ingress, _)) = listener.accept().await {
        let dest = { master_addr.read().await.clone() };
        tokio::spawn(proxy_connection(ingress, dest));
    }

    Ok(())
}

async fn proxy_connection(mut ingress: TcpStream, destination: impl ToSocketAddrs) {
    let mut egress = TcpStream::connect(destination).await.unwrap();
    match tokio::io::copy_bidirectional(&mut ingress, &mut egress).await {
        Ok((to_egress, to_ingress)) => {
            println!("proxied {to_egress} bytes from client {to_ingress} from server");
        }
        Err(e) => {
            println!("error proxying: {e}");
        }
    }
}
