#![feature(type_alias_impl_trait)]

use clap::Parser;
use core::fmt;
use ext::{BufExt, MessageExt};
use futures::{future, stream::Next, Future, FutureExt, SinkExt, StreamExt};
use proto::message::MessageType;
use secp256k1::SecretKey;
use std::collections::HashMap;
use tmq::{dealer::Dealer, router::Router, Multipart};
use uuid::Uuid;

use color_eyre::{eyre::eyre, Result};

use crate::{cli::CliOptions, proto::connection_response::AuthorizationType};

pub mod cli;
pub mod ext;
pub mod proto;

macro_rules! ensure_message_type {
    ($actual: expr, $expected: expr) => {
        let actual = $actual;
        let expected = $expected;
        color_eyre::eyre::ensure!(
            actual == expected,
            "Message type mismatch: expected {:?}, found {:?}",
            expected,
            actual
        )
    };
}

pub struct Network {
    context: tmq::Context,
    secret_key: SecretKey,
    recv_sock: Router,
    connections: HashMap<ConnectionId, Connection>,
}

pub struct Connection {
    endpoint: String,
    id: ConnectionId,
    send_sock: tmq::dealer::Dealer,
}

#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConnectionId(Uuid);

impl fmt::Debug for ConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ConnectionId({:x})", self.0.to_simple())
    }
}

impl fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        <Self as fmt::Debug>::fmt(self, f)
    }
}

impl ConnectionId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl ConnectionId {
    pub fn as_bytes(&self) -> &uuid::Bytes {
        self.0.as_bytes()
    }
}

const PUBLIC_ENDPOINT: &str = "tcp://127.0.0.1:8801";
// const PUBLIC_ENDPOINT: &str = "tcp://71.207.151.214:8800";

// type RecvFuture<'a, S = Dealer> = Next<'a, S>;

type RecvFuture<'a> = impl Future<Output = Option<Result<proto::Message>>> + 'a;

impl Connection {
    pub fn new(endpoint: impl AsRef<str>, context: &tmq::Context) -> Result<Self> {
        let endpoint = endpoint.as_ref().to_owned();
        let send_sock = tmq::dealer(context).connect(&endpoint)?;
        Ok(Self {
            id: ConnectionId::new(),
            endpoint,
            send_sock,
        })
    }

    pub async fn send(
        &mut self,
        message_type: MessageType,
        data: Vec<u8>,
    ) -> Result<RecvFuture<'_>> {
        let message = proto::Message {
            message_type: message_type.into(),
            correlation_id: format!("{:x}", Uuid::new_v4().to_simple()),
            content: data,
        };

        let message_bytes = message.to_bytes();
        let message = vec![&message_bytes];
        self.send_sock.send(message).await?;

        Ok(self.send_sock.next().map(|response| {
            response.map(|res| {
                res.map_err(Into::into).and_then(|mut multi| {
                    multi
                        .pop_front()
                        .ok_or_else(|| eyre!("no frames in multipart message"))?
                        .parse_into::<proto::Message>()
                })
            })
        }))
    }
}

impl Network {
    pub fn new(secret_key: SecretKey) -> Result<Self> {
        let context = tmq::Context::new();
        let recv_sock = tmq::router(&context).bind("tcp://0.0.0.0:8801")?;

        Ok(Self {
            context,
            recv_sock,
            secret_key,
            connections: HashMap::default(),
        })
    }

    pub async fn send(
        &mut self,
        message_type: MessageType,
        data: impl Into<Vec<u8>>,
        connection_id: ConnectionId,
    ) -> Result<RecvFuture<'_>> {
        if let Some(conn) = self.connections.get_mut(&connection_id) {
            Ok(conn.send(message_type, data.into()).await?)
        } else {
            Err(eyre!("unknown connection id : {:?}", connection_id))
        }
    }

    pub async fn send_proto<M: prost::Message + Default>(
        &mut self,
        message_type: MessageType,
        message: &M,
        connection_id: ConnectionId,
    ) -> Result<RecvFuture<'_>> {
        self.send(message_type, message.to_bytes(), connection_id)
            .await
    }

    pub async fn connect_to(&mut self, endpoint: impl AsRef<str>) -> Result<ConnectionId> {
        let mut connection = Connection::new(endpoint, &self.context)?;
        let connect_request = proto::ConnectionRequest {
            endpoint: PUBLIC_ENDPOINT.into(),
        };

        let reply = connection
            .send(MessageType::NetworkConnect, connect_request.to_bytes())
            .await?;

        if let Some(reply) = reply.await {
            let reply: proto::Message = reply?;
            ensure_message_type!(
                reply.message_type(),
                MessageType::AuthorizationConnectionResponse
            );
            println!("{:?}", reply);
            let response = proto::ConnectionResponse::try_parse(&reply.content)?;
            println!("{:?}", response);

            let mut trust_roles = Vec::new();
            let mut challenge_roles = Vec::new();

            for role in response.roles {
                match role.auth_type() {
                    AuthorizationType::Unset => {}
                    AuthorizationType::Trust => trust_roles.push(role.role),
                    AuthorizationType::Challenge => challenge_roles.push(role.role),
                }
            }
            let engine = secp256k1::Secp256k1::new();
            let pubkey = hex::encode(
                &secp256k1::PublicKey::from_secret_key(&engine, &self.secret_key).serialize(),
            );
            if !trust_roles.is_empty() {
                let trust_request = proto::AuthorizationTrustRequest {
                    roles: trust_roles,
                    public_key: pubkey.clone(),
                };

                let response = connection
                    .send(
                        MessageType::AuthorizationTrustRequest,
                        trust_request.to_bytes(),
                    )
                    .await?
                    .await;

                if let Some(response) = response {
                    let response = response?;
                    ensure_message_type!(
                        response.message_type(),
                        MessageType::AuthorizationTrustResponse
                    );
                    println!("Trust request response: {:?}", response);
                }
            }
            if !challenge_roles.is_empty() {
                let challenge_request = proto::AuthorizationChallengeRequest::default();
                let response = connection
                    .send(
                        MessageType::AuthorizationChallengeRequest,
                        challenge_request.to_bytes(),
                    )
                    .await?
                    .await;

                if let Some(response) = response {
                    let response = response?;
                    ensure_message_type!(
                        response.message_type(),
                        MessageType::AuthorizationChallengeResponse
                    );
                    println!("Challenge request response: {:?}", response);
                }
            }
        } else {
            return Err(eyre!("Got no response to connection request"));
        }

        let id = connection.id;
        self.connections.insert(id, connection);
        Ok(id)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;

    let opts = CliOptions::parse();
    let mut rng = rand::thread_rng();
    let key = secp256k1::SecretKey::new(&mut rng);

    let mut network = Network::new(key)?;

    let id = network.connect_to("tcp://127.0.0.1:8800").await?;

    let conn = network.connections.get(&id).unwrap();

    println!("trying to recv");
    // let message = conn.send_sock.recv_bytes(0)?;

    // let message = network.recv_sock.recv_multipart(0)?;
    // let message = network.recv_sock.recv_bytes(0)?;

    // let message = proto::Message::try_parse(&message)?;

    // println!("{:?}", message);

    // let response = message.content.parse_into::<proto::ConnectionResponse>()?;

    // println!("{:?}", response);

    // let response = proto::ConnectionResponse {
    //     status: proto::connection_response::Status::Ok.into(),
    //     ..Default::default()
    // };

    // let ack = proto::NetworkAcknowledgement {
    //     status: proto::network_acknowledgement::Status::Ok.into(),
    // };

    // network.send_proto(MessageType::NetworkAck, &ack, id)?;

    Ok(())
}
