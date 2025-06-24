use std::path::Path;

use anyhow::Result;

use mini_lsm::{LsmStorageOptions, MiniLsm};

use crate::node::{Node, NodeHandle};

pub struct Cluster {
    nodes: Vec<NodeHandle>,
}

impl Cluster {
    pub fn start<P: AsRef<Path>>(paths: Vec<P>) -> Result<Self> {
        let mut handles = Vec::new();
        let mut senders = Vec::new();
        for _ in 0..paths.len() {
            let (tx, _rx) = crossbeam_channel::unbounded();
            senders.push(tx);
        }
        let mut peer_senders = Vec::new();
        for _ in 0..paths.len() {
            let peers = senders.iter().cloned().collect::<Vec<_>>();
            peer_senders.push(peers);
        }
        for (i, path) in paths.into_iter().enumerate() {
            let opts = LsmStorageOptions::default_for_week1_test();
            let storage = MiniLsm::open(path, opts)?;
            let peers = peer_senders[i].clone();
            let (node, tx) = Node::new(i, peers, storage);
            let handle = NodeHandle { tx: tx.clone() };
            handles.push(handle);
            node.run();
        }
        Ok(Self { nodes: handles })
    }

    pub fn node(&self, id: usize) -> Option<&NodeHandle> {
        self.nodes.get(id)
    }
}
