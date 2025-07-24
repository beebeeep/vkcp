use std::sync::Arc;

use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use crate::{
    config,
    grpc::{
        RequestVoteRequest, RequestVoteResponse, SendUpdateRequest, SendUpdateResponse,
        vkcp_server::Vkcp,
    },
};

pub struct Node {
    current_master: Arc<RwLock<String>>,
    id: u32,
}

impl Node {
    pub fn new(cfg: &config::Config, current_master: Arc<RwLock<String>>) -> Self {
        Self {
            current_master,
            id: cfg.peer_id,
        }
    }
}

#[tonic::async_trait]
impl Vkcp for Node {
    async fn send_update(
        &self,
        req: Request<SendUpdateRequest>,
    ) -> Result<Response<SendUpdateResponse>, Status> {
        todo!()
    }

    async fn request_vote(
        &self,
        req: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        todo!()
    }
}
