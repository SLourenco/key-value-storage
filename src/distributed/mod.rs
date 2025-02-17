use crate::distributed::entry::LogEntry;
use crate::distributed::node::{new_node, Leader, Node};
use crate::distributed::rpc::new_rpc;
use crate::storage::bit_cask::{new_bit_cask, BitCask};
use crate::storage::{KVStorage, KV};
use std::collections::HashMap;
use std::io::{Error, ErrorKind};

mod entry;
mod logfile;
pub(crate) mod node;
mod rand;
pub(crate) mod rpc;

pub struct DistributedStorage {
    pub node: Node,
    storage: BitCask,
    distributed: bool,
}

pub fn new_distributed_storage(
    host: &str,
    port: u16,
    data_dir: &str,
    distributed: bool,
) -> Result<DistributedStorage, Error> {
    let node_id = port as u64;
    let nodes_map = HashMap::from([(4000, 4000), (5000, 5000), (6000, 6000)]);
    let nodes = vec![4000, 5000, 6000];
    let rpc = new_rpc(host, nodes_map)?;

    let kv_storage = new_bit_cask(data_dir)?;
    let node = new_node(node_id, rpc.clone(), nodes, kv_storage.clone())?;

    Ok(DistributedStorage {
        node,
        storage: kv_storage,
        distributed,
    })
}

impl DistributedStorage {
    pub fn get(&self, key: usize) -> Result<String, Error> {
        self.storage.get(key)
    }
    pub fn put(&mut self, key: usize, value: String) -> Result<(), Error> {
        if self.distributed {
            if !self.node.can_accept_requests() {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    format!(
                        "Node is not a leader. Leader is node {}",
                        self.node.get_leader()
                    ),
                ));
            }
            let le: LogEntry = Default::default();
            let request = le.format_command("PUT", vec![KV { key, value }]);
            self.node.add_request_to_log(request.as_str())?;
        } else {
            self.storage.put(key, value)?;
        }
        Ok(())
    }
    pub fn delete(&mut self, key: usize) -> Result<(), Error> {
        if self.distributed {
            if !self.node.can_accept_requests() {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    format!(
                        "Node is not a leader. Leader is node {}",
                        self.node.get_leader()
                    ),
                ));
            }
            let le: LogEntry = Default::default();
            let request = le.format_command(
                "DELETE",
                vec![KV {
                    key,
                    value: Default::default(),
                }],
            );
            self.node.add_request_to_log(request.as_str())?;
        } else {
            self.storage.delete(key)?;
        }
        Ok(())
    }
    pub fn range(&self, start: usize, end: usize) -> Result<Vec<KV>, Error> {
        self.storage.range(start, end)
    }
    pub fn batch_put(&mut self, kvs: Vec<KV>) -> Result<(), Error> {
        if self.distributed {
            if !self.node.can_accept_requests() {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    format!(
                        "Node is not a leader. Leader is node {}",
                        self.node.get_leader()
                    ),
                ));
            }
            let le: LogEntry = Default::default();
            let request = le.format_command("BATCH PUT", kvs);
            self.node.add_request_to_log(request.as_str())?;
        } else {
            self.storage.batch_put(kvs)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::distributed::entry::LogEntry;
    use crate::distributed::node::{new_node, Follower};
    use crate::distributed::rpc::{AppendEntriesRequest, VoteRequest};

    #[test]
    fn test_follower_insert_new_entries() {
        let mut node = new_node(
            1,
            Default::default(),
            Default::default(),
            Default::default(),
        )
        .unwrap();
        let entries = vec![LogEntry {
            term: 1,
            entry: "first entry".to_string(),
            entry_idx: 0,
        }];
        let req = AppendEntriesRequest {
            node: 1,
            term: 1,
            leader_id: 154,
            prev_log_idx: 0,
            entries,
            prev_log_term: 0,
            lead_commit: 0,
        };
        let (current_term, result) = node.append_entries(req).unwrap();
        assert_eq!(current_term, 1);
        assert!(result);

        let l = node.get_log();
        assert_eq!(l.len(), 1);
        assert_eq!(l[0].entry, "first entry");
    }

    #[test]
    fn test_follower_reject_old_leader() {
        let mut node = new_node(
            2,
            Default::default(),
            Default::default(),
            Default::default(),
        )
        .unwrap();
        let entries = vec![];
        let req = AppendEntriesRequest {
            node: 2,
            term: 2,
            leader_id: 154,
            prev_log_idx: 0,
            entries: entries.clone(),
            prev_log_term: 0,
            lead_commit: 0,
        };
        let (current_term, result) = node.append_entries(req).unwrap();
        assert_eq!(current_term, 2);
        assert!(result);

        let req2 = AppendEntriesRequest {
            node: 2,
            term: 1,
            leader_id: 700,
            prev_log_idx: 0,
            entries,
            prev_log_term: 0,
            lead_commit: 0,
        };
        let (current_term, result) = node.append_entries(req2).unwrap();
        assert_eq!(current_term, 2);
        assert!(!result);
    }

    #[test]
    fn test_follower_reject_inconsistent_log() {
        let mut node = new_node(
            3,
            Default::default(),
            Default::default(),
            Default::default(),
        )
        .unwrap();
        let entries = vec![];
        let req = AppendEntriesRequest {
            node: 3,
            term: 1,
            leader_id: 154,
            prev_log_idx: 10,
            entries: entries.clone(),
            prev_log_term: 1,
            lead_commit: 0,
        };
        let (_, result) = node.append_entries(req).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_follower_override_entries() {
        let mut node = new_node(
            4,
            Default::default(),
            Default::default(),
            Default::default(),
        )
        .unwrap();
        let entries = vec![LogEntry {
            term: 1,
            entry: "first entry".to_string(),
            entry_idx: 0,
        }];
        let req = AppendEntriesRequest {
            node: 4,
            term: 1,
            leader_id: 154,
            prev_log_idx: 0,
            entries,
            prev_log_term: 0,
            lead_commit: 0,
        };
        let (current_term, result) = node.append_entries(req).unwrap();
        assert_eq!(current_term, 1);
        assert!(result);

        let new_entries = vec![LogEntry {
            term: 1,
            entry: "second entry".to_string(),
            entry_idx: 0,
        }];
        let req2 = AppendEntriesRequest {
            node: 4,
            term: 1,
            leader_id: 154,
            prev_log_idx: 0,
            entries: new_entries,
            prev_log_term: 0,
            lead_commit: 0,
        };
        let (current_term, result) = node.append_entries(req2).unwrap();
        assert_eq!(current_term, 1);
        assert!(result);

        let l = node.get_log();
        assert_eq!(l.len(), 1);
        assert_eq!(l[0].entry, "second entry");
    }

    #[test]
    fn test_follower_vote() {
        let mut node = new_node(
            5,
            Default::default(),
            Default::default(),
            Default::default(),
        )
        .unwrap();
        let req = VoteRequest {
            node: 5,
            term: 1,
            candidate_id: 154,
            last_log_idx: 0,
            last_log_term: 0,
        };
        let (current_term, result) = node.vote(req).unwrap();
        assert_eq!(current_term, 1);
        assert!(result);
    }

    #[test]
    fn test_follower_vote_rejected() {
        let mut node = new_node(
            6,
            Default::default(),
            Default::default(),
            Default::default(),
        )
        .unwrap();
        let req = AppendEntriesRequest {
            node: 4,
            term: 10,
            leader_id: 154,
            prev_log_idx: 0,
            entries: vec![],
            prev_log_term: 0,
            lead_commit: 0,
        };
        let _ = node.append_entries(req).unwrap();

        let req = VoteRequest {
            node: 6,
            term: 1,
            candidate_id: 700,
            last_log_idx: 0,
            last_log_term: 0,
        };
        let (current_term, result) = node.vote(req).unwrap();
        assert_eq!(current_term, 10);
        assert!(!result);
    }

    #[test]
    fn test_follower_vote_rejected_already_voted() {
        let mut node = new_node(
            7,
            Default::default(),
            Default::default(),
            Default::default(),
        )
        .unwrap();
        let req = VoteRequest {
            node: 7,
            term: 1,
            candidate_id: 200,
            last_log_idx: 0,
            last_log_term: 0,
        };
        let (_, result) = node.vote(req).unwrap();
        assert!(result);

        let req2 = VoteRequest {
            node: 7,
            term: 1,
            candidate_id: 700,
            last_log_idx: 0,
            last_log_term: 0,
        };
        let (current_term, result) = node.vote(req2).unwrap();
        assert_eq!(current_term, 1);
        assert!(!result);
    }

    #[test]
    fn test_follower_vote_rejected_outdated_candidate() {
        let mut node = new_node(
            8,
            Default::default(),
            Default::default(),
            Default::default(),
        )
        .unwrap();
        let entries = vec![LogEntry {
            term: 1,
            entry: "first entry".to_string(),
            entry_idx: 0,
        }];
        let req = AppendEntriesRequest {
            node: 4,
            term: 10,
            leader_id: 154,
            prev_log_idx: 0,
            entries,
            prev_log_term: 0,
            lead_commit: 0,
        };
        let _ = node.append_entries(req).unwrap();

        let req = VoteRequest {
            node: 8,
            term: 1,
            candidate_id: 700,
            last_log_idx: 0,
            last_log_term: 0,
        };
        let (current_term, result) = node.vote(req).unwrap();
        assert_eq!(current_term, 10);
        assert!(!result);
    }
}
