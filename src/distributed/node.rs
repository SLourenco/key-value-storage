use crate::distributed::entry::LogEntry;
use crate::distributed::logfile::{
    append_to_file, create_new_file, get_log_filename, read_log_file,
};
use crate::distributed::rand::get_timer_reset;
use crate::distributed::rpc::{AppendEntriesRequest, HTTPNode, VoteRequest};
use crate::storage::bit_cask::BitCask;
use crate::storage::KVStorage;
use std::cmp::max;
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

pub trait Follower {
    fn append_entries(&mut self, req: AppendEntriesRequest) -> Result<(u64, bool), Error>;
    fn vote(&mut self, req: VoteRequest) -> Result<(u64, bool), Error>;
}

pub trait Leader {
    fn add_request_to_log(&mut self, req: &str) -> Result<(), Error>;
    fn request_append_entries(&mut self, nodes: Vec<u64>, rpc: HTTPNode) -> Result<u64, Error>;
}

trait Candidate {
    fn start_election(&mut self, nodes: Vec<u64>, rpc: HTTPNode) -> Result<bool, Error>;
}

#[derive(Clone, Eq, PartialEq)]
pub enum State {
    FOLLOWER,
    LEADER,
    CANDIDATE,
}

struct NodeState {
    state: State,
    current_term: u64,
    voted_for: u64,
    log: Vec<LogEntry>,
    commit_idx: u64,
    last_applied: u64,
    election_timer: i64,
    leader_id: u64,

    // Leader state
    next_idx: HashMap<u64, u64>,
    match_idx: HashMap<u64, u64>,
}

#[derive(Clone)]
pub struct Node {
    node_id: u64,
    state: Arc<Mutex<NodeState>>,

    rpc: HTTPNode,
    other_nodes: Vec<u64>,

    storage: BitCask,
}

pub fn new_node(id: u64, rpc: HTTPNode, nodes: Vec<u64>, storage: BitCask) -> Result<Node, Error> {
    println!("Starting new node {} as follower", id);
    let mut match_idx = HashMap::new();
    for n in &nodes {
        match_idx.insert(*n, 0);
    }

    let created = create_new_file(get_log_filename(id).as_str())?;
    let mut logs = Vec::new();
    let mut commit_idx = 0;
    let mut last_applied = 0;
    if !created {
        println!("Existing log file found. Loading log from file...");
        let saved_logs = read_log_file(get_log_filename(id).as_str())?;
        for (idx, (term, entry)) in saved_logs.iter().enumerate() {
            logs.push(LogEntry {
                term: *term,
                entry: entry.to_string(),
                entry_idx: idx as u64,
            })
        }
        commit_idx = logs.len() as u64;
        last_applied = logs.len() as u64;
    }

    let state = Arc::new(Mutex::new(NodeState {
        state: State::FOLLOWER,
        current_term: 0,
        voted_for: 0,
        log: logs,
        commit_idx,
        last_applied,
        election_timer: get_timer_reset(id),
        next_idx: match_idx.clone(),
        match_idx,
        leader_id: 0,
    }));

    let n = Node {
        node_id: id,
        state,
        rpc: rpc.clone(),
        other_nodes: nodes.clone(),
        storage,
    };

    let mut node = n.clone();
    thread::spawn(move || loop {
        thread::sleep(Duration::from_secs(5));
        let mut state_lock = node.state.lock().unwrap();

        if state_lock.commit_idx > state_lock.last_applied {
            let c_idx = state_lock.commit_idx.clone();
            drop(state_lock);
            let r = node.apply_log(c_idx);
            if r.is_err() {
                println!("Could not apply log: {:?}", r.err().unwrap());
            }
            continue;
        }

        match state_lock.state {
            State::FOLLOWER => {
                state_lock.election_timer = state_lock.election_timer - 5;
                if state_lock.election_timer < 0 {
                    drop(state_lock);
                    println!("Election time out. Node {} is starting election", id);
                    let result = node.start_election(nodes.clone(), rpc.clone());
                    if result.is_ok() {
                        if result.unwrap() {
                            println!("Newly elected leader node {} sending append entries", id);
                            let _ = node.request_append_entries(nodes.clone(), rpc.clone());
                        }
                    }
                }
            }
            State::LEADER => {
                drop(state_lock);
                println!("Leader node {} sending append entries", id);
                let _ = node.request_append_entries(nodes.clone(), rpc.clone());
            }
            State::CANDIDATE => {}
        }
    });

    Ok(n)
}

impl Node {
    // Might be preferable to allow partial apply
    fn apply_log(&mut self, idx: u64) -> Result<(), Error> {
        let mut state_lock = self.state.lock().unwrap();
        let log_file = get_log_filename(self.node_id);
        let mut entries: Vec<(u64, &str)> = Vec::new();
        for i in &state_lock.log[(state_lock.last_applied as usize)..(idx as usize)] {
            entries.push((i.term, i.entry.as_str()));
            let (cmd, values) = i.parse_command(i.entry.as_str())?;
            match cmd.as_str() {
                "BATCH PUT" => {
                    self.storage.batch_put(values)?;
                }
                "PUT" => {
                    let v = values.first().unwrap();
                    self.storage.put(v.key, v.value.clone())?;
                }
                "DELETE" => {
                    self.storage.delete(values.first().unwrap().key)?;
                }
                _ => println!("Command {} not found", cmd),
            }
        }

        append_to_file(log_file.as_str(), entries)?;
        println!("Applied idx {}", idx);
        state_lock.last_applied = idx;
        Ok(())
    }

    pub(crate) fn get_log(&self) -> Vec<LogEntry> {
        self.state.lock().unwrap().log.clone()
    }

    pub(crate) fn can_accept_requests(&self) -> bool {
        self.state.lock().unwrap().state == State::LEADER
    }

    pub(crate) fn get_leader(&self) -> u64 {
        self.state.lock().unwrap().leader_id
    }
}

impl Follower for Node {
    fn append_entries(&mut self, req: AppendEntriesRequest) -> Result<(u64, bool), Error> {
        let mut state_lock = self.state.lock().unwrap();
        println!(
            "Node {}(term {}) received append entries request from node {}(term {}) with commit {}",
            self.node_id, state_lock.current_term, req.leader_id, req.term, req.lead_commit
        );

        if state_lock.current_term > req.term {
            return Ok((state_lock.current_term, false));
        }

        state_lock.election_timer = get_timer_reset(self.node_id);
        state_lock.state = State::FOLLOWER;
        state_lock.current_term = req.term;
        state_lock.leader_id = req.leader_id;

        if req.prev_log_idx > 0 {
            let pl = state_lock.log.get(req.prev_log_idx as usize);
            match pl {
                Some(log_entry) => {
                    if log_entry.term != req.prev_log_term {
                        println!("Rejected append entries due to inconsistent log");
                        return Ok((state_lock.current_term, false));
                    }
                }
                None => {
                    return Ok((state_lock.current_term, false));
                }
            }
        }

        let log_len = state_lock.log.len() as u64;
        for e in req.entries {
            if e.entry_idx < log_len {
                let _ = std::mem::replace(&mut state_lock.log[e.entry_idx as usize], e);
            } else {
                state_lock.log.push(e);
            }
        }

        if req.lead_commit > state_lock.commit_idx {
            let c_idx = req.lead_commit;
            state_lock.commit_idx = c_idx;
            drop(state_lock);
            self.apply_log(c_idx)?;
        }
        Ok((req.term, true))
    }

    fn vote(&mut self, req: VoteRequest) -> Result<(u64, bool), Error> {
        let mut state_lock = self.state.lock().unwrap();
        println!(
            "Node {}(term {}) received request vote from node {}(term {})",
            self.node_id, state_lock.current_term, req.candidate_id, req.term
        );

        if state_lock.current_term > req.term {
            return Ok((state_lock.current_term, false));
        }

        state_lock.current_term = req.term;

        if state_lock.voted_for != 0 && state_lock.voted_for != req.candidate_id {
            println!(
                "Rejected vote since already voted for {}",
                state_lock.voted_for
            );
            return Ok((state_lock.current_term, false));
        }

        let follower_last_log_idx = state_lock.log.len();
        if follower_last_log_idx == 0 {
            state_lock.voted_for = req.candidate_id;
            return Ok((state_lock.current_term, true));
        }

        let follower_last_log = state_lock.log[follower_last_log_idx - 1].clone();
        if req.last_log_idx >= follower_last_log.entry_idx
            && req.last_log_term >= follower_last_log.term
        {
            state_lock.voted_for = req.candidate_id;
            return Ok((state_lock.current_term, true));
        }

        println!("Rejected vote due to inconsistent log");
        Ok((state_lock.current_term, false))
    }
}

impl Candidate for Node {
    fn start_election(&mut self, nodes: Vec<u64>, rpc: HTTPNode) -> Result<bool, Error> {
        let mut state_lock = self.state.lock().unwrap();
        println!("Starting election for node {}", self.node_id);

        state_lock.state = State::CANDIDATE;
        state_lock.current_term = state_lock.current_term + 1;
        state_lock.election_timer = get_timer_reset(self.node_id);
        state_lock.voted_for = self.node_id;

        let majority = (nodes.len() / 2) + 1;

        let last_log = state_lock.log.last();
        let mut last_log_idx = 0;
        let mut last_log_term = 0;
        match last_log {
            Some(log_entry) => {
                last_log_idx = log_entry.entry_idx;
                last_log_term = log_entry.term;
            }
            None => {}
        }

        let mut number_of_votes = 1;
        for n in nodes {
            if n == self.node_id {
                continue;
            }

            let req = VoteRequest {
                node: n,
                candidate_id: self.node_id,
                term: state_lock.current_term,
                last_log_idx,
                last_log_term,
            };
            let result = rpc.request_vote(req);
            if result.is_err() {
                println!("Could not reach node {} to request a vote", n);
                continue;
            }
            let result = result?;
            if result.term > state_lock.current_term {
                state_lock.voted_for = 0;
                state_lock.current_term = result.term;
                state_lock.election_timer = get_timer_reset(self.node_id);
                state_lock.state = State::FOLLOWER;
                break;
            }
            if result.accepted {
                number_of_votes += 1;
            }
            if number_of_votes >= majority {
                state_lock.state = State::LEADER;
                println!("Node {} got majority. Is now leader", self.node_id);
                return Ok(true);
            }
        }

        println!(
            "Election finished without majority for node {}. Reverting to follower",
            self.node_id
        );
        state_lock.state = State::FOLLOWER;
        state_lock.voted_for = 0;
        Ok(false)
    }
}

impl Leader for Node {
    fn add_request_to_log(&mut self, req: &str) -> Result<(), Error> {
        let mut state_lock = self.state.lock().unwrap();
        let last_idx = (state_lock.log.len() + 1) as u64;
        let current_term = state_lock.current_term;

        state_lock.log.push(LogEntry {
            term: current_term,
            entry: req.to_string(),
            entry_idx: last_idx,
        });

        state_lock.commit_idx = last_idx;
        drop(state_lock);
        let applied_counter =
            self.request_append_entries(self.other_nodes.clone(), self.rpc.clone())?;
        let majority = ((self.other_nodes.len() / 2) + 1) as u64;
        if applied_counter >= majority {
            self.apply_log(last_idx)?;
        } else {
            // revert commit
            let mut state_lock = self.state.lock().unwrap();
            state_lock.commit_idx = current_term;
            return Err(Error::new(
                ErrorKind::NotFound,
                "Nodes are inconsistent, command could not be applied".to_string(),
            ));
        }

        Ok(())
    }

    fn request_append_entries(&mut self, nodes: Vec<u64>, rpc: HTTPNode) -> Result<u64, Error> {
        let mut state_lock = self.state.lock().unwrap();
        let mut applied = 0;
        for n in nodes {
            if n == self.node_id {
                applied += 1;
                continue;
            }

            let next_idx = state_lock.next_idx.get(&n).unwrap().clone();
            let match_idx = state_lock.match_idx.get(&n).unwrap().clone();

            let log_idx = state_lock.log.len() as u64;
            let mut prev_log_idx = 0;
            let mut prev_log_term = 0;
            let mut entries = Vec::new();
            if log_idx > next_idx {
                entries = state_lock.log[(next_idx as usize)..(log_idx as usize)].to_vec();
            }
            if next_idx > 0 {
                let prev_entry = state_lock.log[(next_idx - 1) as usize].clone();
                prev_log_idx = prev_entry.entry_idx;
                prev_log_term = prev_entry.term;
            }

            let entries_size = entries.len();
            let req = AppendEntriesRequest {
                node: n,
                term: state_lock.current_term,
                leader_id: self.node_id,
                prev_log_idx,
                prev_log_term,
                entries,
                lead_commit: state_lock.commit_idx,
            };
            let res = rpc.append_entries(req);
            if res.is_err() {
                println!("Could not reach node {} to append entries", n);
                continue;
            }
            let res = res?;
            if res.term > state_lock.current_term {
                state_lock.state = State::FOLLOWER;
                state_lock.current_term = res.term;
                return Ok(0);
            }

            if res.accepted {
                if entries_size > 0 {
                    state_lock.next_idx.insert(n, log_idx);
                    state_lock.match_idx.insert(n, log_idx - 1);
                }
                applied += 1;
            } else {
                let ni = if next_idx > 0 {
                    max(next_idx - 1, match_idx)
                } else {
                    0
                };
                state_lock.next_idx.insert(n, ni);
            }
        }
        Ok(applied)
    }
}
