use std::time::Duration;

use anyhow::{Context, Result};
use autometrics::autometrics;
use tokio::{
    sync::{mpsc, oneshot},
    time,
};
use tonic::{Request, Response, Status};

use crate::{
    config,
    grpc::{
        HeartbeatRequest, HeartbeatResponse, RequestVoteRequest, RequestVoteResponse,
        vkcp_server::Vkcp,
    },
    state_machine::{self},
};

// Server implements GRPC API for controller
pub struct Server {
    actions: mpsc::Sender<state_machine::Message>,
}

impl Server {
    pub fn new(cfg: &config::Config) -> Result<(Self, mpsc::Sender<state_machine::Message>)> {
        let (tx, rx) = mpsc::channel(8);

        let mut sm = state_machine::StateMachine::new(&cfg, rx, tx.clone())
            .context("initializing state machine")?;
        tokio::spawn(async move { sm.run().await });

        Ok((
            Self {
                actions: tx.clone(),
            },
            tx,
        ))
    }
}

#[tonic::async_trait]
impl Vkcp for Server {
    #[autometrics]
    async fn heartbeat(
        &self,
        req: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        time::sleep(Duration::from_millis(100)).await;
        let (tx, rx) = oneshot::channel();
        let msg = state_machine::Message::Heartbeat {
            req: req.into_inner(),
            resp: tx,
        };
        let _ = self.actions.send(msg).await;
        let resp = rx.await.map_err(|e| Status::internal(format!("{e:#}")))?;
        match resp {
            Ok(resp) => Ok(Response::new(resp)),
            Err(err) => Err(Status::internal(format!("{err:#}"))),
        }
    }

    #[autometrics]
    async fn request_vote(
        &self,
        req: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        let (tx, rx) = oneshot::channel();
        let msg = state_machine::Message::RequestVote {
            req: req.into_inner(),
            resp: tx,
        };
        let _ = self.actions.send(msg).await;
        let resp = rx.await.map_err(|e| Status::internal(format!("{e:#}")))?;
        match resp {
            Ok(resp) => Ok(Response::new(resp)),
            Err(err) => Err(Status::internal(format!("{err:#}"))),
        }
    }
}
