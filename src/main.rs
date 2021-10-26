use std::collections::HashSet;

use clap::Parser;

use ext::{BufExt, MessageExt};
use networking::ConnectionId;
use proto::message::MessageType;

use color_eyre::Result;

use crate::cli::CliOptions;
use crate::networking::Network;

pub mod cli;
pub mod ext;
pub mod networking;
pub mod proto;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct CreditcoinNode {
    connection_id: ConnectionId,
    endpoint: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;

    let opts = CliOptions::parse();
    let mut rng = rand::thread_rng();
    let key = secp256k1::SecretKey::new(&mut rng);

    let mut network = Network::with_seeds(key, opts.seeds.as_slice()).await?;

    let mut updates = network.take_update_rx().unwrap();

    let id = network
        .connect_to("tcp://creditcoin-test-node.gluwa.com:8800")
        .await?;

    network.request_peers_of(id).await?;

    let mut stop = network.stop_rx();

    stop.recv().await?;

    Ok(())
}
