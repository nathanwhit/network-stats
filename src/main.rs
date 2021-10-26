use core::fmt;
use std::collections::{HashSet, VecDeque};
use std::hash::Hash;

use clap::Parser;

use ext::{BufExt, MessageExt};
use internment::Intern;
use networking::ConnectionId;
use petgraph::graph::UnGraph;
use petgraph::graphmap::UnGraphMap;
use petgraph_graphml::GraphMl;
use proto::message::MessageType;

use color_eyre::Result;

use crate::cli::CliOptions;
use crate::ext::ResultExt;
use crate::networking::{Network, NetworkEvent};

pub mod cli;
pub mod ext;
pub mod networking;
pub mod proto;

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct CreditcoinNode {
    endpoint: String,
}

impl From<String> for CreditcoinNode {
    fn from(endpoint: String) -> Self {
        Self { endpoint }
    }
}

impl fmt::Display for CreditcoinNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.endpoint)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    pretty_env_logger::init();

    let opts = CliOptions::parse();
    let mut rng = rand::thread_rng();
    let key = secp256k1::SecretKey::new(&mut rng);

    let mut network = Network::with_options(key, &opts).await?;

    let mut updates = network.take_update_rx().unwrap();

    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();

    let seeds: Vec<_> = opts
        .seeds
        .iter()
        .cloned()
        .map(|s| Intern::new(CreditcoinNode::from(s)))
        .collect();
    visited.extend(seeds.clone());
    queue.extend(seeds.clone());

    let mut stop = network.stop_rx();

    let mut topology: UnGraphMap<Intern<CreditcoinNode>, ()> = UnGraphMap::new();

    'a: loop {
        while let Some(node) = queue.pop_front() {
            log::debug!("visiting {}", node);
            match network.connect_to(&node.endpoint).await {
                Ok(node_id) => {
                    log::debug!("requesting peers");
                    network.request_peers_of(node_id).await.log_err()},
                Err(e) => log::error!("failed to connect to node {}: {}", &*node, e),
            }
        }

        tokio::select! {
            _ = stop.recv() => {
                log::warn!("Stopping main loop");
                break 'a;
            }
            update = updates.recv() => {
                if let Some(NetworkEvent::DiscoveredPeers { node, peers }) = update {
                    log::info!("Discovered peers of {}: {:?}", node, peers);
                    let node = Intern::new(node);
                    if !topology.contains_node(node) {
                        topology.add_node(node);
                    }
                    for peer in peers {
                        let peer_node = CreditcoinNode::from(peer.clone());
                        let peer_node = Intern::new(peer_node);
                        if node == peer_node {
                            continue;
                        }
                        if !topology.contains_node(peer_node) {
                            topology.add_node(peer_node);
                        }

                        topology.add_edge(node, peer_node, ());

                        if visited.insert(peer_node.clone()) {
                            queue.push_back(peer_node);
                        }
                    }
                }
            }
        }
    }

    if let Some(dot_out) = &opts.dot {
        let dot = petgraph::dot::Dot::with_config(&topology, &[petgraph::dot::Config::EdgeNoLabel]);
        std::fs::write(dot_out, format!("{:?}", dot))?;
    }

    if let Some(ml_out) = &opts.graphml {
        let graphml = GraphMl::new(&topology)
            .export_node_weights_display()
            .to_string();
        std::fs::write(ml_out, graphml)?;
    }

    drop(network);

    updates.recv().await;

    Ok(())
}
