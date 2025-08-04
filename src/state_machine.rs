use std::ops::Range;
use std::time::Duration;

use crate::config;
use crate::config::Config;
use crate::grpc;
use crate::grpc::vkcp_client::VkcpClient;
use anyhow::anyhow;
use anyhow::{Context, Result};
use fred::prelude as fredis;
use fred::prelude::ClientLike;
use metrics::{counter, gauge};
use rand::Rng;
use rand::rng;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task;
use tokio::time;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

const DEFAULT_HEARTBEAT_PERIOD: u64 = 1500; // main leader loop timeout
const DEFAULT_HEARTBEAT_TIMEOUT: u64 = 300;
const DEFAULT_ELECTION_TIMEOUT_MS_MIN: u64 = 4000;
const DEFAULT_ELECTION_TIMEOUT_MS_MAX: u64 = 6000;
const DEFAULT_VK_SERVER_TIMEOUT: u64 = 500;
const DEFAULT_VK_SERVER_CHECK_PERIOD: u64 = 2000;

const METRIC_HEARTBEAT_FAIL: &str = "vkcp.election.follower.heartbeat.fails";
const METRIC_HEARTBEAT_OK: &str = "vkcp.election.follower.heartbeat.oks";
const METRIC_NODE_STATE: &str = "vkcp.election.state";
const METRIC_NODE_TERM: &str = "vkcp.election.term";
const METRIC_HEARTBEATS_SENT_OK: &str = "vkcp.election.leader.heartbeat.sent";
const METRIC_HEARTBEATS_SENT_ERR: &str = "vkcp.election.leader.heartbeat.error";
const METRIC_SERVER_HEALTHY: &str = "vkcp.server.healthcheck.healthy";

#[derive(Copy, Clone)]
pub enum MachineState {
    Leader = 0,
    Follower = 1,
    Candidate = 2,
}

struct PeerState {
    client: VkcpClient<Channel>,
    id: u32,
}

#[derive(Debug)]
struct ServerState {
    inner: grpc::ServerState,

    context: Option<CancellationToken>, // to signal proxies to terminate connection toward demoted master
    replicaof: Option<String>,          // only on leader, for failovers
}

pub enum Message {
    // GRPC api
    RequestVote {
        req: grpc::RequestVoteRequest,
        resp: oneshot::Sender<Result<grpc::RequestVoteResponse>>,
    },
    Heartbeat {
        req: grpc::HeartbeatRequest,
        resp: oneshot::Sender<Result<grpc::HeartbeatResponse>>,
    },

    // messages from internal async jobs
    ReceiveVote((u32, grpc::RequestVoteResponse)),
    HeartbeatResponse(grpc::HeartbeatResponse),

    // requests from proxying subsystem
    GetCurrentMaster(oneshot::Sender<Option<(String, CancellationToken)>>),
}

pub struct StateMachine {
    id: u32,
    rx_msgs: mpsc::Receiver<Message>,
    tx_msgs: mpsc::Sender<Message>,
    peers: Vec<PeerState>,
    quorum: usize,
    heartbeat_period: Duration,
    heartbeat_timeout: Duration,
    election_timeout_range: Range<u64>,
    vk_server_timeout: Duration,
    vk_server_check_period: Duration,
    tags: Vec<(String, String)>,

    // valkey servers status
    servers: Vec<ServerState>,
    preferred_master: Option<String>,

    // volatile state
    state: MachineState,
    term: u64,
    next_follower_update: Instant,
    updates_pending: usize,
    next_servers_ping: Instant,

    // volatile state on candidate
    votes_received: usize,
    voted_for: Option<u32>,
}

impl StateMachine {
    pub fn new(
        cfg: &Config,
        rx_msgs: mpsc::Receiver<Message>,
        tx_msgs: mpsc::Sender<Message>,
    ) -> Result<Self> {
        let mut r = Self {
            id: cfg.peer_id,
            rx_msgs,
            tx_msgs,
            quorum: (cfg.peers.len() / 2 + 1),
            peers: Self::init_peers(cfg.peers.clone())?,
            election_timeout_range: cfg
                .election_timeout_ms_min
                .unwrap_or(DEFAULT_ELECTION_TIMEOUT_MS_MIN)
                ..cfg
                    .election_timeout_ms_max
                    .unwrap_or(DEFAULT_ELECTION_TIMEOUT_MS_MAX),
            heartbeat_period: Duration::from_millis(
                cfg.heartbeat_period_ms.unwrap_or(DEFAULT_HEARTBEAT_PERIOD),
            ),
            heartbeat_timeout: Duration::from_millis(
                cfg.heartbeat_timeout_ms
                    .unwrap_or(DEFAULT_HEARTBEAT_TIMEOUT),
            ),
            vk_server_timeout: Duration::from_millis(
                cfg.vk_server_timeout_ms
                    .unwrap_or(DEFAULT_VK_SERVER_TIMEOUT),
            ),
            vk_server_check_period: Duration::from_millis(
                cfg.vk_server_check_period_ms
                    .unwrap_or(DEFAULT_VK_SERVER_CHECK_PERIOD),
            ),
            state: MachineState::Follower,
            next_follower_update: Instant::now(),
            next_servers_ping: Instant::now(),
            votes_received: 0,
            servers: cfg
                .servers
                .clone()
                .into_iter()
                .map(|addr| ServerState {
                    inner: grpc::ServerState {
                        addr,
                        ..Default::default()
                    },
                    context: None,
                    replicaof: None,
                })
                .collect(),
            preferred_master: None,
            term: 0,
            voted_for: None,
            updates_pending: 0,
            tags: cfg
                .extra_tags
                .clone()
                .map_or(vec![], |tags| tags.into_iter().collect()),
        };
        r.tags.push((String::from("peer_id"), format!("{}", r.id)));
        Ok(r)
    }

    pub async fn run(&mut self) {
        loop {
            gauge!(METRIC_NODE_STATE, &self.tags).set(self.state as i32);
            gauge!(METRIC_NODE_TERM, &self.tags).set(self.term as f64);
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
        self.maybe_update_servers()
            .await
            .context("updating servers")?;
        self.maybe_update_followers()
            .await
            .context("updating followers")?;

        select! {
            _ = time::sleep(self.heartbeat_period) => {},
            Some(msg) = self.rx_msgs.recv() => {
                match msg {
                    Message::RequestVote { req, resp } => {
                        let _ = resp.send(self.process_request_vote(req).await.context("voting for candidate")); // NB: not clear what shall we do here?
                    },
                    Message::ReceiveVote(_) => {
                        // don't care as we are leader already
                    },
                    Message::HeartbeatResponse(resp) => {
                        if resp.term > self.term {
                            info!(term = resp.term, "got heartbeat response with greater term, converting to follower");
                            self.convert_to_follower(resp.term);
                        }
                        if resp.success {
                            self.updates_pending -= 1;
                        }
                    },
                    Message::Heartbeat{req, resp } => {
                        if req.term > self.term {
                            info!(new_leader_id = req.leader_id, new_leader_term = req.term, "found new leader with greated term, converting to follower");
                            self.convert_to_follower(req.term);
                            let _ = resp.send(self.heartbeat(req).await.context("processing Heartbeat"));
                        } else {
                            // this really isn't supposed to happen, yet just in case we step
                            info!(offender_id = req.leader_id, offender_term = req.term, "found unexpected leader");
                            self.convert_to_follower(req.term);
                            // but don't acknowledge the heartbeat, leader will either send us a new one next time,
                            // or step down too if it failed to get a quorum
                            let _ = resp.send(Ok(grpc::HeartbeatResponse { term: self.term, success: false }));
                        }
                    },
                    Message::GetCurrentMaster(resp) => {
                        let _ = resp.send(self.get_current_master());
                    },
                }
            }
        }
        Ok(())
    }

    async fn run_follower(&mut self) -> Result<()> {
        select! {
            _ = time::sleep_until(self.get_election_timeout()) => {
                self.convert_to_candidate().await;
            },
            Some(msg) = self.rx_msgs.recv() => {
                match msg {
                    Message::RequestVote { req, resp } => {
                        let _ = resp.send(self.process_request_vote(req).await.context("requesting vote"));
                    },
                    Message::ReceiveVote(_) => {
                        // don't care as follower
                    },
                    Message::HeartbeatResponse { .. } => {
                        // do not care as follower
                    },
                    Message::Heartbeat{req, resp } => {
                        let _ = resp.send(self.heartbeat(req).await.context("processing AppendEntries"));
                    },
                    Message::GetCurrentMaster(resp) => {
                        let _ = resp.send(self.get_current_master());
                    },
                }
            }
        }
        Ok(())
    }

    async fn run_candidate(&mut self) -> Result<()> {
        select! {
            _ = time::sleep_until(self.get_election_timeout()) => {
                info!("heartbeat timed out");
                self.convert_to_candidate().await;
            },
            Some(msg) = self.rx_msgs.recv() => {
                match msg {
                    Message::RequestVote { req, resp } => {
                        let _ = resp.send(self.process_request_vote(req).await.context("requesting vote"));
                    },
                    Message::HeartbeatResponse { .. } => {
                        // do not care as candidate
                    },
                    Message::Heartbeat{req, resp } => {
                        if req.term >= self.term {
                            // leader was elected and already send Heartbeat
                            debug!(term = req.term, "got AppendEntries from new leader");
                            self.convert_to_follower(req.term);
                        }
                        let _ = resp.send(self.heartbeat(req).await.context("processing Heartbeat"));
                    },
                    Message::ReceiveVote((peer_id, vote)) => {
                        info!(voter = peer_id, votes_received = self.votes_received, vote_granted = vote.vote_granted, "vote result");
                        if vote.term > self.term {
                            // we are stale
                            debug!(vote = ?vote, my_term = self.term,  "got RequestVote reply with greater term");
                            self.convert_to_follower(vote.term)

                        } else if vote.term == self.term && vote.vote_granted {
                            self.votes_received += 1;
                            if self.votes_received >= self.quorum {
                                self.convert_to_leader();
                            }
                        }
                    },
                    Message::GetCurrentMaster(resp) => {
                        // Candidate nodes shall not proxy connections to master. This is a way to ensure that if the zone with current VK master
                        // becomes isolated, so that new proxy leader won't be able to reach it and demote to slave, local proxies (which would be locked
                        // in candidate state because they cannot get quorum) won't proxy write requests to old master thus creating data split-brain.
                        // As a matter of fact, some data inconsistency is still possible because network partitioning cannot be detected instantly,
                        // yet we are trying to minimize the impact
                        let _ = resp.send(None);
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
            current_master = req
                .servers
                .iter()
                .find(|s| s.healthy && s.is_master)
                .map_or("N/A", |s| s.addr.as_str()),
            "got heartbeat"
        );
        if req.term < self.term || req.leader_id >= self.peers.len() as u32 {
            // stale leader or incorrect data, reject RPC
            debug!(
                term = self.term,
                req_term = req.term,
                leader = req.leader_id,
                "heartbeat from stale leader"
            );
            counter!(METRIC_HEARTBEAT_FAIL, &self.tags).increment(1);
            return Ok(grpc::HeartbeatResponse {
                term: self.term,
                success: false,
            });
        }
        if req.term > self.term {
            self.set_term(req.term);
        }
        self.update_servers(req.servers);
        counter!(METRIC_HEARTBEAT_OK, &self.tags).increment(1);

        Ok(grpc::HeartbeatResponse {
            term: self.term,
            success: true,
        })
    }

    async fn process_request_vote(
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
            debug!(
                req_term = req.term,
                my_term = self.term,
                candidate = req.candidate_id,
                "got RequestVote with greater term"
            );
            self.convert_to_follower(req.term);
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

    async fn convert_to_candidate(&mut self) {
        info!("converting to candidate");
        self.state = MachineState::Candidate;
        self.term += 1;
        self.voted_for = Some(self.id);
        self.votes_received = 1;

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
    }

    fn convert_to_follower(&mut self, new_term: u64) {
        info!(new_term = new_term, "converting to follower");
        self.set_term(new_term);
        self.state = MachineState::Follower;
        self.voted_for = None;
    }

    fn convert_to_leader(&mut self) {
        info!("converting to leader");
        self.state = MachineState::Leader;
        self.voted_for = None;
        self.next_servers_ping = Instant::now();
        self.next_follower_update = Instant::now();
        self.updates_pending = 0;
    }

    fn set_term(&mut self, term: u64) {
        self.term = term;
        self.voted_for = None;
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
                let msg = Message::ReceiveVote((peer_id, repl));
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

    async fn maybe_update_servers(&mut self) -> Result<()> {
        if Instant::now() < self.next_servers_ping {
            return Ok(());
        }
        self.next_servers_ping = Instant::now() + self.vk_server_check_period;

        for server in self.servers.iter_mut() {
            match server.update_state(self.vk_server_timeout).await {
                Err(e) => {
                    error!(
                        server = &server.inner.addr,
                        error = format!("{e:#}"),
                        "getting server info"
                    );
                }
                Ok(_) => {}
            }

            let mut tags = self.tags.clone();
            tags.push((String::from("server"), server.inner.addr.clone()));
            tags.push((
                String::from("role"),
                String::from(if server.inner.is_master {
                    "master"
                } else if server.inner.healthy {
                    "replica"
                } else {
                    "unknown"
                }),
            ));
            gauge!(METRIC_SERVER_HEALTHY, &tags).set(if server.inner.healthy { 1 } else { 0 });
            debug!(state = ?server, "server state");
        }

        self.update_replication_topology()
            .await
            .context("updating replication topology")?;

        Ok(())
    }

    fn select_preferred_master(&mut self) {
        if let Some(m) = self.preferred_master.as_ref() {
            // we already have selected master...
            if self
                .servers
                .iter()
                .find(|s| &s.inner.addr == m && s.inner.healthy && s.inner.is_master)
                .is_some()
            {
                // ... it is healthy and actually master
                return;
            }
        }

        // we need to elect new master, select healthy replica with maximum offset
        // NB: if all servers are masters (i.e. they just started and they weren't initially configured for replication
        // as we don't require that), they will have offset of 0, so below code will select last one in list
        // as preferred master
        let mut candidate: Option<&ServerState> = None;
        for s in self.servers.iter() {
            if s.inner.healthy && s.inner.offset >= candidate.map_or(0, |v| v.inner.offset) {
                candidate = Some(s);
            }
        }
        self.preferred_master = candidate.map(|v| v.inner.addr.clone());
    }

    async fn update_replication_topology(&mut self) -> Result<()> {
        self.select_preferred_master();
        if self.preferred_master.is_none() {
            warn!("cannot find a suitable new master, all nodes are unhealthy");
            return Ok(());
        }

        debug!(addr = ?self.preferred_master, "preferred master");

        // now, ensure replication topology
        let preferred_master = self.preferred_master.as_ref().unwrap();
        for server in self.servers.iter_mut() {
            if &server.inner.addr == preferred_master {
                if !server.inner.is_master {
                    if let Err(e) = server.promote(self.vk_server_timeout).await {
                        warn!(
                            server = server.inner.addr,
                            error = format!("{e:#}"),
                            "failed to promote master"
                        );
                    } else {
                        info!(server = server.inner.addr, "promoting master");
                    }
                }
            } else {
                if server
                    .replicaof
                    .as_ref()
                    .map_or(true, |s| s != preferred_master)
                {
                    if let Err(e) = server
                        .replicate_from(preferred_master, self.vk_server_timeout)
                        .await
                    {
                        warn!(
                            server = server.inner.addr,
                            new_master = preferred_master,
                            error = format!("{e:#}"),
                            "failed to set replication"
                        );
                    } else {
                        info!(
                            server = server.inner.addr,
                            new_master = preferred_master,
                            "setting replication"
                        );
                    }
                }
            }
        }

        Ok(())
    }

    async fn maybe_update_followers(&mut self) -> Result<()> {
        if Instant::now() < self.next_follower_update {
            return Ok(());
        }

        if self.updates_pending >= self.quorum {
            // it is time to deliver next update, but we haven't yet get
            // responses from quorum of followers, so we step down to candidate stat
            self.convert_to_candidate().await;
            return Err(anyhow!(
                "did not received quorum of heartbeats: {} still pending",
                self.updates_pending
            ));
        }
        self.next_follower_update = Instant::now() + self.heartbeat_period;
        self.updates_pending = self.peers.len() - 1;
        debug!("sending heartbeats");

        for peer in self.peers.iter() {
            if peer.id == self.id {
                continue;
            }
            let mut client = peer.client.clone();
            let peer_id = peer.id;
            let update_timeout = Instant::now() + self.heartbeat_timeout;
            let req = grpc::HeartbeatRequest {
                term: self.term,
                leader_id: self.id,
                servers: self.servers.iter().map(|x| x.inner.clone()).collect(),
            };
            let msgs = self.tx_msgs.clone();
            let mut tags = self.tags.clone();
            tags.push((String::from("follower_id"), format!("{}", peer_id)));
            task::spawn(async move {
                select! {
                    _ = time::sleep_until(update_timeout) => {
                        warn!(peer_id = peer_id, "Update timed out");
                    },
                    resp = client.heartbeat(req) => {
                        match resp {
                            Ok(resp) => {
                                counter!(METRIC_HEARTBEATS_SENT_OK, &tags).increment(1);
                                let resp = resp.into_inner();
                                debug!(follower = peer_id, success = resp.success, "follower Heartbeat response");
                                let _ = msgs.send(Message::HeartbeatResponse(resp)).await;
                            }
                            Err(err) => {
                                counter!(METRIC_HEARTBEATS_SENT_ERR, &tags).increment(1);
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

    fn get_election_timeout(&mut self) -> Instant {
        Instant::now()
            .checked_add(Duration::from_millis(
                rng().random_range(self.election_timeout_range.clone()),
            ))
            .unwrap()
    }

    fn get_current_master(&self) -> Option<(String, CancellationToken)> {
        self.servers
            .iter()
            .find(|s| s.inner.is_master && s.inner.healthy)
            .map(|x| (x.inner.addr.clone(), x.context.as_ref().unwrap().clone()))
    }

    fn update_servers(&mut self, new_servers: Vec<grpc::ServerState>) {
        for new in new_servers {
            if let Some(old) = self.servers.iter_mut().find(|s| s.inner.addr == new.addr) {
                if !new.healthy {
                    // server became unhealthy, terminate all connections
                    old.cancel();
                }
                if !old.inner.is_master && new.is_master && new.healthy {
                    // server was promoted and is healthy, setup context for clients
                    old.context = Some(CancellationToken::new());
                }
                old.inner = new;
            }
        }
    }
}

impl ServerState {
    async fn connect(&mut self, timeout: Duration) -> Result<fredis::Client> {
        let mut cfg = fredis::Config::from_url(&format!("redis://{}", self.inner.addr))?;
        cfg.fail_fast = true;
        let mut builder = fredis::Builder::from_config(cfg);
        builder.with_connection_config(|cfg| {
            cfg.internal_command_timeout = timeout;
            cfg.connection_timeout = timeout;
            cfg.max_command_attempts = 1;
            cfg.tcp = fredis::TcpConfig {
                nodelay: Some(true),
                ..Default::default()
            };
        });
        builder.set_policy(fredis::ReconnectPolicy::new_linear(1, 10, 10));

        let client = builder.build().context("building client")?;
        match client.init().await {
            Ok(_) => Ok(client),
            Err(e) => {
                warn!(
                    server = self.inner.addr,
                    error = format!("{e:#}"),
                    "connection to server failed"
                );
                self.inner.healthy = false;
                self.cancel();
                Err(e).context("connecting to server")
            }
        }
    }

    async fn update_state(&mut self, timeout: Duration) -> Result<()> {
        self.inner.healthy = false;
        let client = self.connect(timeout).await?;
        let info: String = match client.info(Some(fred::types::InfoKind::Replication)).await {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    server = self.inner.addr,
                    error = format!("{e:#}"),
                    "INFO command failed"
                );
                self.inner.healthy = false;
                self.cancel();
                return Ok(());
            }
        };

        if let Err(e) = client.quit().await {
            warn!(
                server = self.inner.addr,
                error = format!("{e:#}"),
                "closing connection"
            );
        }

        let (mut master_host, mut master_port) = (None, None);
        for line in info.split("\r\n") {
            let mut p = line.split(":");
            match (p.next(), p.next()) {
                (Some("role"), Some("master")) => {
                    self.inner.is_master = true;
                    if self.context.is_none() {
                        self.context = Some(CancellationToken::new());
                    }
                }
                (Some("role"), Some("slave")) => {
                    self.inner.is_master = false;
                    self.replicaof = None;
                    self.cancel() // if there were any context, it should be cancelled now because it's slave
                }
                (Some("slave_repl_offset"), Some(offset)) => {
                    self.inner.offset = offset.parse().unwrap_or_default();
                }
                (Some("master_repl_offset"), Some(offset)) => {
                    self.inner.offset = offset.parse().unwrap_or_default();
                }
                (Some("master_host"), Some(host)) => {
                    master_host = Some(String::from(host));
                }
                (Some("master_port"), Some(port)) => {
                    master_port = Some(port.parse::<u16>().unwrap_or_default());
                }
                _ => {}
            }
        }
        self.inner.healthy = true;
        self.replicaof = match (master_host, master_port) {
            (Some(h), Some(p)) => Some(format!("{h}:{p}")),
            _ => None,
        };
        Ok(())
    }

    async fn promote(&mut self, timeout: Duration) -> Result<()> {
        let client = self.connect(timeout).await?;
        let _: () = client
            .custom(fred::cmd!("REPLICAOF"), vec!["NO", "ONE"])
            .await
            .context("running REPLICAOF NO ONE")?;

        // self.inner.is_master = true; // not really needed?
        Ok(())
    }

    async fn replicate_from(&mut self, addr: &str, timeout: Duration) -> Result<()> {
        let client = self.connect(timeout).await?;
        let (host, port) = Self::get_host_port(addr);
        let _: () = client
            .custom(fred::cmd!("REPLICAOF"), vec![host, &port.to_string()])
            .await
            .context("running REPLICAOF")?;

        Ok(())
    }

    /// cancel cancels underlying token so that everybody using that server get signalled
    /// that something is wrong with server and it shall stop proxying there.
    fn cancel(&mut self) {
        if let Some(ctx) = self.context.take() {
            ctx.cancel();
        }
    }

    fn get_host_port<'a>(addr: &'a str) -> (&'a str, u16) {
        let mut p = addr.split(":");
        match (p.next(), p.next()) {
            (Some(h), Some(p)) => (h, p.parse().unwrap_or_default()),
            _ => {
                panic!("invalid address");
            }
        }
    }
}
