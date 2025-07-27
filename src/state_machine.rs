use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use crate::config;
use crate::grpc;
use crate::grpc::vkcp_client::VkcpClient;
use anyhow::{Context, Result};
use fred::prelude as fredis;
use fred::prelude::ClientLike;
use rand::Rng;
use rand::rng;
use tokio::select;
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task;
use tokio::time;
use tokio::time::Instant;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

const HEARTBEAT_PERIOD: Duration = Duration::from_millis(1500); // main leader loop timeout
const HEARTBEAT_TIMEOUT: Duration = Duration::from_millis(300);
const ELECTION_TIMEOUT_MS: Range<u64> = 2500..3000;
const VK_SERVER_TIMEOUT: Duration = Duration::from_millis(500);
const VK_SERVER_CHECK_PERIOD: Duration = Duration::from_millis(2000);

pub enum MachineState {
    Leader,
    Follower,
    Candidate,
}

struct PeerState {
    client: VkcpClient<Channel>,
    id: u32,
}

#[derive(Debug)]
struct ServerState {
    addr: String,
    healthy: bool,
    is_master: bool,
    offset: u64,
}

pub enum Message {
    RequestVote {
        req: grpc::RequestVoteRequest,
        resp: oneshot::Sender<Result<grpc::RequestVoteResponse>>,
    },
    Heartbeat {
        req: grpc::HeartbeatRequest,
        resp: oneshot::Sender<Result<grpc::HeartbeatResponse>>,
    },

    // messages from internal async jobs
    ReceiveVote(grpc::RequestVoteResponse),
    HeartbeatResponse(grpc::HeartbeatResponse),
}

pub struct StateMachine {
    id: u32,
    rx_msgs: mpsc::Receiver<Message>,
    tx_msgs: mpsc::Sender<Message>,
    peers: Vec<PeerState>,
    quorum: u32,
    election_timeout: Instant,

    // valkey servers status
    servers: Vec<ServerState>,
    current_master: Arc<RwLock<String>>,

    // volatile state
    state: MachineState,
    term: u64,
    next_follower_update: Instant,
    next_servers_ping: Instant,

    // volatile state on candidate
    votes_received: u32,
    voted_for: Option<u32>,
}

impl StateMachine {
    pub fn new(
        id: u32,
        peers: Vec<config::PeerConfig>,
        current_master: Arc<RwLock<String>>,
        servers: Vec<String>,
        rx_msgs: mpsc::Receiver<Message>,
        tx_msgs: mpsc::Sender<Message>,
    ) -> Result<Self> {
        Ok(Self {
            id,
            rx_msgs,
            tx_msgs,
            quorum: (peers.len() / 2 + 1) as u32,
            peers: Self::init_peers(peers)?,
            election_timeout: Self::next_election_timeout(None),
            state: MachineState::Follower,
            next_follower_update: Instant::now(),
            next_servers_ping: Instant::now(),
            votes_received: 0,
            servers: servers
                .into_iter()
                .map(|addr| ServerState {
                    healthy: false,
                    addr,
                    is_master: false,
                    offset: 0,
                })
                .collect(),
            current_master,
            term: 0,
            voted_for: None,
        })
    }

    pub async fn run(&mut self) {
        loop {
            let err = match self.state {
                MachineState::Leader => self.run_leader().await,
                MachineState::Follower => self.run_follower().await,
                MachineState::Candidate => self.run_candidate().await,
            };
            match err {
                Ok(_) => {}
                Err(err) => error!(error = format!("{err:#}"), "got error"),
            }
        }
    }

    async fn run_leader(&mut self) -> Result<()> {
        self.maybe_update_servers_health()
            .await
            .context("updating followers")?;
        self.maybe_update_followers()
            .await
            .context("updating commitIndex")?;

        select! {
            _ = time::sleep(HEARTBEAT_PERIOD) => {},
            Some(msg) = self.rx_msgs.recv() => {
                match msg {
                    Message::RequestVote { req, resp } => {
                        let _ = resp.send(self.request_vote(req).await.context("voting for candidate")); // NB: not clear what shall we do here?
                    },
                    Message::ReceiveVote(_) => {
                        // don't care as we are leader already
                    },
                    Message::HeartbeatResponse(resp) => {
                        if resp.term > self.term {
                            info!(term = resp.term, "got heartbeat response with greater term, converting to follower");
                            self.convert_to_follower();
                        }
                    },
                    Message::Heartbeat{req, resp } => {
                        if req.term > self.term {
                            info!(new_leader_id = req.leader_id, new_leader_term = req.term, "found new leader with greated term, converting to follower");
                            self.convert_to_follower();
                            let _ = resp.send(self.heartbeat(req).await.context("processing Heartbeat"));
                        } else {
                            info!(offender_id = req.leader_id, offender_term = req.term, "found unexpected leader");
                        }
                    },
                }
            }
        }
        Ok(())
    }

    async fn run_follower(&mut self) -> Result<()> {
        select! {
            _ = time::sleep_until(self.election_timeout) => {
                self.convert_to_candidate().await.context("converting to candidate")?;
            },
            Some(msg) = self.rx_msgs.recv() => {
                match msg {
                    Message::RequestVote { req, resp } => {
                        let _ = resp.send(self.request_vote(req).await.context("requesting vote"));
                    },
                    Message::ReceiveVote(_) => {
                        // don't care as follower
                    },
                    Message::HeartbeatResponse { .. } => {
                        // do not care as follower
                    },
                    Message::Heartbeat{req, resp } => {
                        if req.term > self.term {
                            self.term = req.term;
                        }
                        let _ = resp.send(self.heartbeat(req).await.context("processing AppendEntries"));
                    },
                }
            }
        }
        Ok(())
    }

    async fn run_candidate(&mut self) -> Result<()> {
        select! {
            _ = time::sleep_until(self.election_timeout) => {
                self.convert_to_candidate().await.context("converting to candidate")?;
            },
            Some(msg) = self.rx_msgs.recv() => {
                match msg {
                    Message::RequestVote { req, resp } => {
                        let _ = resp.send(self.request_vote(req).await.context("requesting vote"));
                    },
                    Message::HeartbeatResponse { .. } => {
                        // do not care as candidate
                    },
                    Message::Heartbeat{req, resp } => {
                        if req.term > self.term {
                            // leader was elected and already send Heartbeat
                            self.term = req.term;
                            debug!(term = req.term, "got AppendEntries with greater term");
                            self.convert_to_follower();
                        }
                        let _ = resp.send(self.heartbeat(req).await.context("processing Heartbeat"));
                    },
                    Message::ReceiveVote(vote) => {
                        info!(votes_received = self.votes_received, vote_granted = vote.vote_granted, "vote result");
                        if vote.term > self.term {
                            // we are stale
                            self.term = vote.term;
                            debug!(term = vote.term, "got RequestVote reply with greater term");
                            self.convert_to_follower()
                        } else if vote.term == self.term && vote.vote_granted {
                            self.votes_received += 1;
                            if self.votes_received >= self.quorum {
                                self.convert_to_leader();
                            }
                        }
                    },
                }
            }
        }
        Ok(())
    }

    async fn heartbeat(&mut self, req: grpc::HeartbeatRequest) -> Result<grpc::HeartbeatResponse> {
        debug!(
            leader_id = req.leader_id,
            leader_term = req.term,
            "got heartbeat"
        );
        if req.term < self.term || req.leader_id >= self.servers.len() as u32 {
            // stale leader or incorrect data, reject RPC
            return Ok(grpc::HeartbeatResponse {
                term: self.term,
                success: false,
            });
        }
        self.election_timeout = Self::next_election_timeout(None);
        {
            self.current_master
                .write()
                .await
                .clone_from(&self.servers[req.leader_id as usize].addr);
        }

        Ok(grpc::HeartbeatResponse {
            term: self.term,
            success: true,
        })
    }

    async fn request_vote(
        &mut self,
        req: grpc::RequestVoteRequest,
    ) -> Result<grpc::RequestVoteResponse> {
        if req.term < self.term {
            // candidate is stale
            return Ok(grpc::RequestVoteResponse {
                term: self.term,
                vote_granted: false,
            });
        }
        if req.term > self.term {
            // candidate is more recent
            self.term = req.term;
            debug!(term = req.term, "got RequestVote with greater term");
            self.convert_to_follower();
        }

        let vote_granted = match self.voted_for {
            None if self.term <= req.term => true,
            Some(candidate) => candidate == req.candidate_id,
            _ => false,
        };
        debug!(
            vote_granted = vote_granted,
            my_term = self.term,
            candidate_term = req.term,
            candidate = req.candidate_id,
            voted_for = self.voted_for,
            "voting"
        );
        Ok(grpc::RequestVoteResponse {
            term: self.term,
            vote_granted,
        })
    }

    async fn convert_to_candidate(&mut self) -> Result<()> {
        info!("converting to candidate");
        self.state = MachineState::Candidate;
        self.term += 1;
        self.voted_for = Some(self.id);
        self.votes_received = 1;
        self.election_timeout = Self::next_election_timeout(None);

        // request votes from all peers in parallel
        for peer in self.peers.iter() {
            if self.id == peer.id {
                continue;
            }
            task::spawn(Self::request_vote_from_peer(
                peer.client.clone(),
                peer.id,
                self.tx_msgs.clone(),
                self.term,
                self.id,
            ));
        }
        Ok(())
    }

    fn convert_to_follower(&mut self) {
        info!("converting to follower");
        self.state = MachineState::Follower;
    }

    fn convert_to_leader(&mut self) {
        info!("converting to leader");
        self.state = MachineState::Leader;
    }

    async fn request_vote_from_peer(
        mut client: VkcpClient<Channel>,
        peer_id: u32,
        msgs: mpsc::Sender<Message>,
        term: u64,
        candidate_id: u32,
    ) {
        let req = grpc::RequestVoteRequest { term, candidate_id };
        match client.request_vote(req).await {
            Ok(repl) => {
                let repl = repl.into_inner();
                let msg = Message::ReceiveVote(grpc::RequestVoteResponse {
                    term: repl.term,
                    vote_granted: repl.vote_granted,
                });
                let _ = msgs.send(msg).await;
            }
            Err(err) => {
                error!(
                    candidate = candidate_id,
                    peer_id = peer_id,
                    error = format!("{err:#}"),
                    "requesting vote from peer",
                );
            }
        }
    }

    async fn maybe_update_servers_health(&mut self) -> Result<()> {
        if Instant::now() < self.next_servers_ping {
            return Ok(());
        }
        self.next_servers_ping = Instant::now() + VK_SERVER_CHECK_PERIOD;

        for server in self.servers.iter_mut() {
            match server.update_state().await {
                Err(e) => {
                    error!(
                        server = &server.addr,
                        error = format!("{e:#}"),
                        "getting server info"
                    );
                    continue;
                }
                Ok(_) => {
                    debug!(state = ?server, "server state");
                }
            }
        }
        Ok(())
    }

    async fn maybe_update_followers(&mut self) -> Result<()> {
        if Instant::now() < self.next_follower_update {
            return Ok(());
        }
        self.next_follower_update = Instant::now() + HEARTBEAT_PERIOD;

        for peer in self.peers.iter() {
            if peer.id == self.id {
                continue;
            }
            let mut client = peer.client.clone();
            let peer_id = peer.id;
            let update_timeout = Instant::now() + HEARTBEAT_TIMEOUT;
            let current_master = { self.current_master.read().await.clone() };
            let req = grpc::HeartbeatRequest {
                term: self.term,
                leader_id: self.id,
                valkey_master: current_master,
            };
            let msgs = self.tx_msgs.clone();
            task::spawn(async move {
                select! {
                    _ = time::sleep_until(update_timeout) => {
                        warn!(peer_id = peer_id, "Update timed out");
                    },
                    resp = client.heartbeat(req) => {
                        match resp {
                            Ok(resp) => {
                                let _ = msgs.send(Message::HeartbeatResponse(resp.into_inner())).await;
                            }
                            Err(err) => {
                                error!(peer_id = peer_id, error = format!("{err:#}"), "sending Heartbeat");
                            }
                        }
                    }
                };
            });
        }
        Ok(())
    }

    fn init_peers(peer_configs: Vec<config::PeerConfig>) -> Result<Vec<PeerState>> {
        let mut peers = Vec::with_capacity(peer_configs.len());
        for peer in peer_configs.into_iter() {
            println!("connected to {}", peer.addr);
            peers.push(PeerState {
                id: peer.id,
                client: VkcpClient::new(Channel::from_shared(peer.addr)?.connect_lazy()),
            })
        }
        Ok(peers)
    }

    fn next_election_timeout(r: Option<Range<u64>>) -> Instant {
        Instant::now()
            .checked_add(Duration::from_millis(
                rng().random_range(r.unwrap_or(ELECTION_TIMEOUT_MS)),
            ))
            .unwrap()
    }
}

impl ServerState {
    async fn update_state(&mut self) -> Result<()> {
        let mut cfg = fredis::Config::from_url(&format!("redis://{}", self.addr)).unwrap();
        cfg.fail_fast = true;
        let client = fredis::Builder::from_config(cfg)
            .with_connection_config(|cfg| {
                cfg.connection_timeout = VK_SERVER_TIMEOUT;
                cfg.tcp = fredis::TcpConfig {
                    nodelay: Some(true),
                    ..Default::default()
                };
            })
            .build()
            .unwrap();
        if let Err(e) = client.init().await {
            warn!(
                server = self.addr,
                error = format!("{e:#}"),
                "connection to server failed"
            );
            self.healthy = false;
            return Ok(());
        }

        let info: String = match client.info(Some(fred::types::InfoKind::Replication)).await {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    server = self.addr,
                    error = format!("{e:#}"),
                    "INFO command failed"
                );
                self.healthy = false;
                return Ok(());
            }
        };

        if let Err(e) = client.quit().await {
            warn!(
                server = self.addr,
                error = format!("{e:#}"),
                "closing connection"
            );
        }

        for line in info.split("\r\n") {
            let mut p = line.split(":");
            match (p.next(), p.next()) {
                (Some("role"), Some("master")) => self.is_master = true,
                (Some("slave_repl_offset"), Some(offset)) => {
                    self.offset = offset.parse().unwrap_or_default();
                }
                (Some("master_repl_offset"), Some(offset)) => {
                    self.offset = offset.parse().unwrap_or_default();
                }
                _ => {}
            }
        }
        self.healthy = true;

        Ok(())
    }
}
