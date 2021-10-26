use clap::Parser;

use ext::{BufExt, MessageExt};
use proto::message::MessageType;

use color_eyre::Result;

use crate::cli::CliOptions;
use crate::networking::Network;

pub mod cli;
pub mod ext;
pub mod networking;
pub mod proto;

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;

    let _opts = CliOptions::parse();
    let mut rng = rand::thread_rng();
    let key = secp256k1::SecretKey::new(&mut rng);

    let mut network = Network::new(key)?;

    let id = network.connect_to("tcp://127.0.0.1:8800").await?;

    let response = network
        .send_request(
            MessageType::GossipGetPeersRequest,
            proto::GetPeersRequest::default().to_bytes(),
            id,
        )?
        .await?;

    println!("RESPONSE = {:?}", response);

    let ack = response
        .content
        .parse_into::<proto::NetworkAcknowledgement>()?;
    println!("ack = {:?}", ack);

    let mut stop = network.stop_rx();

    stop.recv().await?;

    Ok(())
}
