use core::fmt;
use std::collections::{HashSet, VecDeque};
use std::hash::Hash;
use std::net::SocketAddr;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;

use dashmap::DashMap;
use futures::{Future, SinkExt, StreamExt};
use internment::Intern;
use petgraph::graph::NodeIndex;
use petgraph::graphmap::UnGraphMap;
use petgraph::stable_graph::StableUnGraph;
use petgraph_graphml::GraphMl;

use color_eyre::Result;
use pin_project::pin_project;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, RwLock};
use tokio_tungstenite::tungstenite::Message;

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
}

impl From<String> for CreditcoinNode {
    fn from(endpoint: String) -> Self {
        Self {
            endpoint: Intern::new(endpoint),
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

async fn handle_connection(
    stream: TcpStream,
    addr: SocketAddr,
    mut stop: broadcast::Receiver<()>,
    topology: NetworkTopology,
) -> Result<()> {
    let mut ws_stream = tokio_tungstenite::accept_async(stream).await?;

    log::debug!("Websocket connection established: {}", addr);
    loop {
        tokio::select! {
            _ = stop.recv() => {
                log::debug!("Stopping websocket handler");
                break;
            }
            msg = ws_stream.next() => {
                match msg {
                    Some(msg) => match msg {
                        Ok(msg) => match msg {
                            Message::Ping(ping) => ws_stream.send(Message::Pong(ping)).await.log_err(),
                            Message::Pong(_) => {},
                            Message::Text(s) => {
                                log::debug!("received message from websocket stream: {}", s);
                                if s == "update" {
                                    let graph = json::Graph::new(&*topology.read().await);
                                    let json = serde_json::to_string(&graph)?;
                                    ws_stream.send(Message::Text(json)).await.log_err();
                                }
                            },
                            Message::Close(close) => {
                                log::debug!("Received close from websocket: {:?}", close);
                                break;
                            },
                            Message::Binary(b) => {
                                log::warn!("received unexpected binary message from websocket: {:?}", b);
                            }
                        }
                        Err(e) => log::error!("received error from websocket stream: {}", e)
                    }
                    None => break,
                }
            }
        }
    }

    Ok(())
}

#[derive(Debug)]
enum Listener {
    Enabled(TcpListener),
    Disabled,
}

#[pin_project(project = MaybeStreamProj)]
enum MaybeStream<'a> {
    Stream(Pin<Box<dyn Future<Output = tokio::io::Result<(TcpStream, SocketAddr)>> + 'a>>),
    Empty,
}

impl<'a> Future for MaybeStream<'a> {
    type Output = tokio::io::Result<(TcpStream, SocketAddr)>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.project() {
            MaybeStreamProj::Stream(s) => s.as_mut().poll(cx),
            MaybeStreamProj::Empty => std::task::Poll::Pending,
        }
    }
}

impl Listener {
    async fn new(opts: &CliOptions) -> Result<Self> {
        if let Some(addr) = &opts.server {
            Ok(Listener::Enabled(TcpListener::bind(addr).await?))
        } else {
            Ok(Listener::Disabled)
        }
    }

    fn accept(&self) -> MaybeStream<'_> {
        match self {
            Listener::Enabled(listener) => MaybeStream::Stream(Box::pin(listener.accept())),
            Listener::Disabled => MaybeStream::Empty,
        }
    }
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

    let listener = Listener::new(&opts).await?;

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

            conn = listener.accept() => {
                log::info!("Got new connection");
                match conn {
                    Ok((stream, addr)) => {
                        tokio::spawn(handle_connection(stream, addr, network.stop_rx(), topology.clone()));
                    },
                    Err(e) => {
                        log::error!("Error on websocket stream: {}", e);
                    }
                }
            }
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
