use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use crate::grpc;
use crate::grpc::vkcp_client::VkcpClient;
use anyhow::{Context, Result, anyhow};
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
const ELECTION_TIMEOUT_MS: Range<u64> = 2500..3000;

pub enum ServerState {
    Leader,
    Follower,
    Candidate,
}

struct PeerState {
    client: VkcpClient<Channel>,
    id: u32,
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
}

pub struct StateMachine {
    id: u32,
    rx_msgs: mpsc::Receiver<Message>,
    tx_msgs: mpsc::Sender<Message>,
    peers: Vec<PeerState>,
    quorum: u32,
    election_timeout: Instant,

    // valkey servers status
    servers: Vec<String>,
    current_master: Arc<RwLock<String>>,

    // volatile state
    state: ServerState,
    term: u64,

    // volatile state on candidate
    votes_received: u32,
    voted_for: Option<u32>,
}

impl StateMachine {
    pub fn new(
        id: u32,
        peers: Vec<String>,
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
            election_timeout: Self::next_election_timeout(Some(100..200)), // such short timeout may cause unnecessary elections on startup (also it's bugged)
            state: ServerState::Follower,
            votes_received: 0,
            servers,
            current_master,
            term: 0,
            voted_for: None,
        })
    }

    pub async fn run(&mut self) {
        loop {
            let err = match self.state {
                ServerState::Leader => self.run_leader().await,
                ServerState::Follower => self.run_follower().await,
                ServerState::Candidate => self.run_candidate().await,
            };
            match err {
                Ok(_) => {}
                Err(err) => error!(error = format!("{err:#}"), "got error"),
            }
        }
    }

    async fn maybe_update_servers_health(&mut self) -> Result<()> {
        todo!("go through all valkeys and get their status");
    }

    async fn run_leader(&mut self) -> Result<()> {
        self.maybe_update_servers_health()
            .await
            .context("updating followers")?;
        self.update_followers()
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
                    Message::Heartbeat{req, resp } => {
                        if req.term > self.term {
                            // leader was elected and already send Heartbeat
                            self.term = req.term;
                            self.set_term(req.term).await.context("setting new term")?;
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
        if req.term < self.term || req.leaderID >= self.servers.len() {
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
                .clone_from(self.servers[req.leaderID]);
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

        if self.voted_for.map_or(true, |v| v == req.candidate_id) && self.term <= req.term {
            // if we haven't voted or or already voted for that candidate, vote for candidate
            // if it's log is at least up-to-date as ours
            self.voted_for = Some(req.candidate_id);
            return Ok(grpc::RequestVoteResponse {
                term: self.term,
                vote_granted: true,
            });
        }
        Ok(grpc::RequestVoteResponse {
            term: self.term,
            vote_granted: false,
        })
    }

    async fn convert_to_candidate(&mut self) -> Result<()> {
        info!("converting to candidate");
        self.state = ServerState::Candidate;
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
        self.state = ServerState::Follower;
    }

    fn convert_to_leader(&mut self) {
        info!("converting to leader");
        self.state = ServerState::Leader;
    }

    async fn request_vote_from_peer(
        mut client: VkcpClient<Channel>,
        peer_id: u32,
        msgs: mpsc::Sender<Message>,
        term: u64,
        candidate_id: u32,
    ) {
        let req = grpc::RequestVoteRequest {
            term,
            candidate_id: candidate_id as u32,
        };
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

    async fn send_entries(&mut self, peer: PeerID, entries: Vec<grpc::LogEntry>) -> Result<()> {
        let mut client = self.peers[peer].client.clone();
        let msgs = self.tx_msgs.clone();
        let _self = format!("{self}");
        let prev_log_idx = self.peers[peer].next_idx - 1;

        let entries_count = entries.len();
        let update_timeout = Instant::now() + UPDATE_TIMEOUT;
        self.peers[peer].update_timeout = Some(update_timeout);
        self.peers[peer].next_heartbeat = Instant::now() + IDLE_TIMEOUT;
        let prev_log_term = if prev_log_idx == 0 {
            0
        } else {
            self.storage
                .get_log_entry(prev_log_idx)
                .await
                .context("getting previous log entry")?
                .term
        };
        let req = grpc::AppendEntriesRequest {
            term: self.storage.current_term(),
            leader_id: self.id as u32,
            prev_log_index: prev_log_idx as u64,
            prev_log_term,
            entries,
            leader_commit: self.commit_idx as u64,
        };
        task::spawn(async move {
            select! {
                _ = time::sleep_until(update_timeout) => {
                    warn!(follower = peer, "AppendEntries timed out")
                },
                resp = client.append_entries(req) => {
                    match resp {
                        Ok(resp) => {
                            let _ = msgs
                                .send(Message::AppendEntriesResponse {
                                    peer_id: peer,
                                    replicated_index: if resp.into_inner().success {
                                        Some(prev_log_idx + entries_count)
                                    } else {
                                        None
                                    },
                                })
                                .await;
                        }
                        Err(err) => {
                            // retries are supposed to be part of raft logic itself
                            error!(follower = peer, error = format!("{err:#}"), "sending AppendEntries" );
                        }
                    }
                }
            }
        });

        Ok(())
    }

    async fn maybe_apply_log(&mut self) -> Result<bool> {
        if self.commit_idx <= self.last_applied_idx {
            return Ok(false);
        }
        self.last_applied_idx += 1;
        debug_assert!(
            self.last_applied_idx <= self.storage.last_log_idx(),
            "(applying log entries) last_applied_idx > log last idx"
        );

        debug!(index = self.last_applied_idx, "applying log entry");
        let cmd = self
            .storage
            .get_log_entry(self.last_applied_idx)
            .await
            .context("fetching latest log entry")?
            .command
            .clone()
            .unwrap();
        let old_value = self
            .storage
            .set(cmd.key, cmd.value)
            .await
            .context("applyng log entry to storage")?;
        if let Some(chan) = self.pending_transactions.remove(&self.last_applied_idx) {
            let _ = chan.send(old_value);
        }
        Ok(true)
    }

    async fn update_followers(&mut self) -> Result<()> {
        let last_log_idx = self.storage.last_log_idx();
        for peer_id in 0..self.peers.len() {
            if peer_id == self.id {
                continue;
            }

            let peer = &mut self.peers[peer_id];
            let ok_to_update = peer.update_timeout.map_or(true, |x| Instant::now() >= x);
            if !ok_to_update {
                continue;
            }

            if peer.next_idx <= last_log_idx {
                // peer is lagging
                debug!(
                    follower = peer_id,
                    my_last_log_idx = last_log_idx,
                    follower_next_idx = peer.next_idx,
                    "updating follower"
                );
                let entries = self
                    .storage
                    .get_logs_since(peer.next_idx)
                    .await
                    .context("getting entries for follower")?;
                self.send_entries(peer_id, entries)
                    .await
                    .context("sending entries to follower")?;
            } else if Instant::now() >= peer.next_heartbeat {
                self.send_entries(peer_id, Vec::new())
                    .await
                    .context("sending heartbeat")?;
            }
        }
        Ok(())
    }

    async fn maybe_update_commit_idx(&mut self) -> Result<()> {
        debug_assert!(
            self.commit_idx <= self.storage.last_log_idx(),
            "(updating commitIndex) commitIndex <= log len"
        );
        if self.commit_idx == self.storage.last_log_idx() {
            return Ok(());
        }
        let mut i = self.storage.last_log_idx();
        while i > self.commit_idx {
            let in_sync = self.peers.iter().filter(|p| p.match_idx >= i).count();
            if in_sync >= self.quorum as usize
                && self
                    .storage
                    .get_log_entry(i)
                    .await
                    .context("getting log entry")?
                    .term
                    == self.storage.current_term()
            {
                debug!(index = i, in_sync = in_sync, "got quorum on log entry");
                self.commit_idx = i;
                return Ok(());
            }
            i -= 1;
        }
        Ok(())
    }

    fn init_peers(addrs: Vec<String>) -> Result<Vec<VkcpClient<Channel>>> {
        let mut peers = Vec::with_capacity(addrs.len());
        for addr in addrs.into_iter() {
            println!("connected to {}", addr);
            peers.push(VkcpClient::new(Channel::from_shared(addr)?.connect_lazy()))
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
