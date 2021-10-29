use core::fmt;
use std::collections::{HashSet, VecDeque};
use std::hash::Hash;
use std::net::SocketAddr;
use std::ops::Deref;

use std::sync::Arc;
use std::time::Duration;

use clap::Parser;

use dashmap::DashMap;
use hyper::{Body, Request, Response};
use internment::Intern;
use petgraph::graph::NodeIndex;
use petgraph::stable_graph::StableUnGraph;
use petgraph_graphml::GraphMl;

use color_eyre::{Report, Result};
use tokio::sync::RwLock;

use crate::cli::CliOptions;
use crate::ext::{FutureExt, ResultExt};
use crate::networking::{Network, NetworkEvent};

pub mod cli;
pub mod ext;
pub mod json;
pub mod networking;
pub mod proto;

#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct CreditcoinNode {
    endpoint: Intern<String>,
    tip: Option<u64>,
}

impl From<String> for CreditcoinNode {
    fn from(endpoint: String) -> Self {
        Self {
            endpoint: Intern::new(endpoint),
            tip: None,
        }
    }
}

impl fmt::Debug for CreditcoinNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreditcoinNode")
            .field("endpoint", &*self.endpoint)
            .finish()
    }
}

impl fmt::Display for CreditcoinNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.endpoint)
    }
}

type NetworkTopology = Arc<RwLock<StableUnGraph<CreditcoinNode, ()>>>;

async fn topology_request(
    _req: Request<Body>,
    topology: NetworkTopology,
) -> Result<Response<Body>> {
    log::warn!("GOT REQUEST");
    let graph = json::Graph::from(&*topology.read().await);
    let json = serde_json::to_string(&graph)?;
    let response = Response::builder()
        .header(hyper::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(Body::from(json))?;
    Ok(response)
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    pretty_env_logger::init();

    let mut opts = CliOptions::parse();
    let mut rng = rand::thread_rng();
    let key = secp256k1::SecretKey::new(&mut rng);

    let seeds: Vec<_> = std::mem::take(&mut opts.seeds)
        .iter()
        .cloned()
        .map(CreditcoinNode::from)
        .collect();

    let network = Arc::new(Network::with_options(key, &opts).await?);

    let mut updates = network.take_update_rx().unwrap();

    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    visited.extend(seeds.clone());
    queue.extend(seeds.clone());

    let mut stop = network.stop_rx();

    let node_ids: DashMap<String, NodeIndex> = DashMap::new();

    let topology: StableUnGraph<CreditcoinNode, ()> = StableUnGraph::default();

    let topology = Arc::new(RwLock::new(topology));

    let addr: SocketAddr = opts
        .server
        .as_ref()
        .map(|s| s.as_str())
        .unwrap_or_else(|| "127.0.0.1:4321")
        .parse()?;

    let topo_clone = topology.clone();
    let make_service = hyper::service::make_service_fn(move |_| {
        let topology = topo_clone.clone();

        async move {
            Ok::<_, Report>(hyper::service::service_fn(move |req| {
                topology_request(req, topology.clone())
            }))
        }
    });

    if opts.server.is_some() {
        let server = hyper::Server::bind(&addr).serve(make_service);
        // let server = hyper::Server::bind(&addr).serve(make_service);

        // let listener = Listener::new(&opts).await?;
        let shutdown = server.with_graceful_shutdown({
            let mut stop = network.stop_rx();
            async move { stop.recv().await.log_err() }
        });

        tokio::spawn(shutdown);
    }

    'a: loop {
        while let Some(node) = queue.pop_front() {
            log::debug!("visiting {}", node);

            if let Ok(()) = stop.try_recv() {
                log::warn!("Stopping main loop");
                break 'a;
            }
            let network = Arc::clone(&network);
            let mut stop = network.stop_rx();
            tokio::spawn(async move {
                match network.connect_to(&*node.endpoint).await {
                    Ok(node_id) => {
                        let mut timer = tokio::time::interval(Duration::from_secs(60));
                        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                        loop {
                            tokio::select! {
                                _ = timer.tick() => {
                                    if let Err(e) = network
                                        .request_peers_of(node_id)
                                        .timeout(Duration::from_secs(55))
                                        .await
                                    {
                                        log::error!("received error: {}", e);
                                        break;
                                    }
                                }
                                _ = stop.recv() => {
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => log::error!("failed to connect to {}: {}", &node, e),
                }
            });
        }

        tokio::select! {
            _ = stop.recv() => {
                log::warn!("Stopping main loop");
                break 'a;
            }
            update = updates.recv() => {
                if let Some(NetworkEvent::DiscoveredPeers { node, peers }) = update {
                    log::info!("Discovered peers of {}: {:?}", node, peers);
                    let mut topo = topology.write().await;
                    let node_id = if let Some(id) = node_ids.get(&*node.endpoint) {
                        *id
                    } else {
                        let id = topo.add_node(node);
                        node_ids.insert(node.endpoint.deref().clone(), id);
                        id
                    };
                    let peerset: HashSet<_> = peers.into_iter().map(|nod| {
                        if let Some(id) = node_ids.get(&nod) {
                            *id
                        } else {
                            let id = topo.add_node(CreditcoinNode::from(nod.clone()));
                            node_ids.insert(nod, id);
                            id
                        }
                    }).collect();
                    let topology_peers: HashSet<_> = topo
                        .neighbors(node_id)
                        .into_iter()
                        .collect();
                    let new_peers = &peerset - &topology_peers;
                    let stale_peers = &topology_peers - &peerset;

                    for peer in new_peers {

                        if node_id == peer {
                            continue;
                        }

                        topo.add_edge(node_id, peer, ());

                        let peer = topo.node_weight(peer).unwrap();

                        if visited.insert(peer.clone()) {
                            queue.push_back(peer.clone());
                        }
                    }
                    for peer in stale_peers {
                        if let Some(edge) = topo.find_edge(node_id, peer) {
                            topo.remove_edge(edge);
                        }
                    }
                }
            }

            // conn = listener.accept() => {
            //     log::info!("Got new connection");
            //     match conn {
            //         Ok((stream, addr)) => {
            //             tokio::spawn(handle_connection(stream, addr, network.stop_rx(), topology.clone()));
            //         },
            //         Err(e) => {
            //             log::error!("Error on websocket stream: {}", e);
            //         }
            //     }
            // }
        }
    }

    let topo = topology.read().await;
    if let Some(dot_out) = &opts.dot {
        log::info!("Writing graphviz output");
        let dot = petgraph::dot::Dot::with_config(&*topo, &[petgraph::dot::Config::EdgeNoLabel]);
        std::fs::write(dot_out, format!("{:?}", dot))?;
    }

    if let Some(ml_out) = &opts.graphml {
        log::info!("Writing graphml output");
        let graphml = GraphMl::new(&*topo)
            .export_node_weights_display()
            .to_string();
        std::fs::write(ml_out, graphml)?;
    }

    if let Some(json_out) = &opts.json {
        log::info!("Writing json output");
        let graph = json::Graph::new(&*topo);
        let json = serde_json::to_vec(&graph)?;

        std::fs::write(json_out, json)?;
    }

    drop(network);

    updates.recv().await;

    Ok(())
}
