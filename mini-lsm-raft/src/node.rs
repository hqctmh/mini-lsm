use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use crossbeam_channel::{unbounded, Receiver, Sender, select};
use rand::Rng;

use mini_lsm::MiniLsm;

/// Command replicated by Raft.
#[derive(Clone, Debug)]
pub enum Command {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, Debug)]
struct LogEntry {
    term: u64,
    command: Command,
}

#[derive(Clone, Debug)]
enum Message {
    RequestVote { term: u64, candidate_id: usize, last_log_index: usize, last_log_term: u64 },
    RequestVoteResponse { term: u64, vote_granted: bool },
    AppendEntries { term: u64, leader_id: usize, prev_log_index: usize, prev_log_term: u64, entries: Vec<LogEntry>, leader_commit: usize },
    AppendEntriesResponse { term: u64, success: bool },
    Client(Command),
    Stop,
}

pub struct Node {
    id: usize,
    peers: Vec<Sender<Message>>,
    rx: Receiver<Message>,
    role: Role,
    term: u64,
    voted_for: Option<usize>,
    log: Vec<LogEntry>,
    commit_index: usize,
    storage: MiniLsm,
}

impl Node {
    pub fn new(id: usize, peers: Vec<Sender<Message>>, storage: MiniLsm) -> (Self, Sender<Message>) {
        let (tx, rx) = unbounded();
        let node = Node {
            id,
            peers,
            rx,
            role: Role::Follower,
            term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            storage,
        };
        (node, tx)
    }

    fn broadcast(&self, msg: Message) {
        for peer in &self.peers {
            let _ = peer.send(msg.clone());
        }
    }

    fn apply(&mut self, entry: &LogEntry) -> Result<()> {
        match &entry.command {
            Command::Put(k, v) => self.storage.put(k, v)?,
            Command::Delete(k) => self.storage.delete(k)?,
        }
        Ok(())
    }

    pub fn run(mut self) {
        thread::spawn(move || {
            let mut rng = rand::thread_rng();
            let mut election_deadline = Instant::now() + Duration::from_millis(rng.gen_range(150..300));
            loop {
                if let Ok(msg) = self.rx.recv_timeout(Duration::from_millis(10)) {
                    match msg {
                        Message::Stop => break,
                        Message::Client(cmd) => {
                            if self.role == Role::Leader {
                                let entry = LogEntry { term: self.term, command: cmd };
                                self.log.push(entry.clone());
                                self.broadcast(Message::AppendEntries {
                                    term: self.term,
                                    leader_id: self.id,
                                    prev_log_index: self.log.len() - 1,
                                    prev_log_term: entry.term,
                                    entries: vec![entry.clone()],
                                    leader_commit: self.commit_index,
                                });
                                self.commit_index = self.log.len();
                                let _ = self.apply(&entry);
                            }
                        }
                        Message::AppendEntries { term, leader_id: _, prev_log_index: _, prev_log_term: _, entries, leader_commit } => {
                            if term >= self.term {
                                self.term = term;
                                self.role = Role::Follower;
                                election_deadline = Instant::now() + Duration::from_millis(rng.gen_range(150..300));
                                for entry in entries {
                                    if entry.term == self.term {
                                        self.log.push(entry.clone());
                                        if self.log.len() <= leader_commit {
                                            self.commit_index = self.log.len();
                                            let _ = self.apply(&entry);
                                        }
                                    }
                                }
                            }
                        }
                        Message::RequestVote { term, candidate_id, last_log_index: _, last_log_term: _ } => {
                            if term > self.term {
                                self.term = term;
                                self.role = Role::Follower;
                                self.voted_for = None;
                            }
                            if self.voted_for.is_none() && term == self.term {
                                self.voted_for = Some(candidate_id);
                                self.send_to(candidate_id, Message::RequestVoteResponse { term, vote_granted: true });
                            } else {
                                self.send_to(candidate_id, Message::RequestVoteResponse { term: self.term, vote_granted: false });
                            }
                        }
                        Message::RequestVoteResponse { term, vote_granted } => {
                            if self.role == Role::Candidate && term == self.term && vote_granted {
                                // majority not precisely tracked; become leader on first vote for simplicity
                                self.role = Role::Leader;
                            }
                        }
                        _ => {}
                    }
                }
                if Instant::now() >= election_deadline {
                    election_deadline = Instant::now() + Duration::from_millis(rng.gen_range(150..300));
                    self.role = Role::Candidate;
                    self.term += 1;
                    self.voted_for = Some(self.id);
                    self.broadcast(Message::RequestVote {
                        term: self.term,
                        candidate_id: self.id,
                        last_log_index: self.log.len(),
                        last_log_term: self.log.last().map(|e| e.term).unwrap_or(0),
                    });
                }
            }
        });
    }

    fn send_to(&self, id: usize, msg: Message) {
        if let Some(peer) = self.peers.get(id) {
            let _ = peer.send(msg);
        }
    }
}

pub struct NodeHandle {
    tx: Sender<Message>,
}

impl NodeHandle {
    pub fn propose(&self, cmd: Command) {
        let _ = self.tx.send(Message::Client(cmd));
    }

    pub fn stop(&self) {
        let _ = self.tx.send(Message::Stop);
    }
}
