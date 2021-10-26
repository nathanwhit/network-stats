use crate::ext::{BufExt, MessageExt};
use crate::proto::{self, message::MessageType};
use core::fmt;
use futures::{Future, FutureExt, SinkExt, StreamExt};
use secp256k1::SecretKey;
use std::collections::HashMap;
use tmq::{dealer::Dealer, Multipart};
use tokio::sync::broadcast;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use uuid::Uuid;

use crate::proto::connection_response::AuthorizationType;
use color_eyre::{eyre::eyre, Result};

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

pub struct Network {
    context: tmq::Context,
    secret_key: SecretKey,
    connections: HashMap<ConnectionId, Connection>,
    command_tx: mpsc::UnboundedSender<NetworkCommand>,
    update_tx: mpsc::UnboundedSender<NetworkEvent>,
    stop_tx: broadcast::Sender<()>,
}

pub struct Connection {
    endpoint: String,
    id: ConnectionId,
    tx: mpsc::UnboundedSender<Command>,
    task: Option<JoinHandle<()>>,
}

const PUBLIC_ENDPOINT: &str = "tcp://127.0.0.1:8801";
// const PUBLIC_ENDPOINT: &str = "tcp://71.207.151.214:8800";

// type RecvFuture<'a, S = Dealer> = Next<'a, S>;

#[derive(Debug)]
pub enum Command {
    Send {
        message: Multipart,
        response_demand: Option<ResponseDemand>,
    },
}

#[derive(Debug)]
pub struct ResponseDemand {
    correlation_id: String,
    tx: oneshot::Sender<proto::Message>,
}

pub struct ResponseFuture {
    rx: oneshot::Receiver<proto::Message>,
}

impl Future for ResponseFuture {
    type Output = Result<proto::Message, oneshot::error::RecvError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.rx.poll_unpin(cx)
    }
}

type ResponseQueue = HashMap<String, oneshot::Sender<proto::Message>>;

impl Connection {
    pub fn create(
        endpoint: impl AsRef<str>,
        context: &tmq::Context,
        command_tx: mpsc::UnboundedSender<NetworkCommand>,
        update_tx: mpsc::UnboundedSender<NetworkEvent>,
        stop_rx: broadcast::Receiver<()>,
    ) -> Result<Connection> {
        let endpoint = endpoint.as_ref().to_owned();
        let id = ConnectionId::new();
        let sock = tmq::dealer(context).connect(&endpoint)?;
        let (tx, queue) = mpsc::unbounded_channel();

        let internal_tx = tx.clone();

        let task = tokio::spawn(async {
            let mut response_queue: ResponseQueue = HashMap::default();
            let mut queue = queue;
            let mut sock = sock;
            let command_tx = command_tx;
            let update_tx = update_tx;
            let mut stop_rx = stop_rx;
            let internal_tx = internal_tx;

            loop {
                tokio::select! {
                    message = queue.recv() => {
                        Self::handle_command(message, &mut sock, &mut response_queue).await;
                    }
                    incoming = sock.next() => {
                        println!("incoming {:?}", incoming);
                        Self::handle_message(incoming, &mut response_queue, &internal_tx, &command_tx, &update_tx).await;
                    }
                    _ = stop_rx.recv() => {
                        println!("stopping connection");
                        let message_bytes = proto::Message {
                            message_type: MessageType::NetworkDisconnect.into(),
                            content: proto::DisconnectMessage::default().to_bytes(),
                            correlation_id: correlation_id(),
                        }.to_bytes();

                        let message = vec![&message_bytes];
                        sock.send(message).await.unwrap();
                        break;
                    }
                }
            }
        });

        let connection = Connection {
            endpoint: endpoint.clone(),
            id,
            tx,
            task: Some(task),
        };

        Ok(connection)
    }

    async fn handle_message(
        message: Option<Result<Multipart, tmq::TmqError>>,
        response_queue: &mut ResponseQueue,
        command_sender: &mpsc::UnboundedSender<Command>,
        network_command_sender: &mpsc::UnboundedSender<NetworkCommand>,
        network_update_sender: &mpsc::UnboundedSender<NetworkEvent>,
    ) {
        match message {
            Some(Ok(mut message)) => {
                if message.len() > 1 {
                    println!("Message longer than expected");
                }
                if let Some(frame) = message.pop_back() {
                    let message = proto::Message::try_parse(&*frame);
                    match message {
                        Ok(message) => {
                            if let Some(tx) = response_queue.remove(&message.correlation_id) {
                                if let Err(e) = tx.send(message) {
                                    println!("Failed to send response over channel: {:?}", e)
                                }
                            } else {
                                println!("Non-response message! {:?}", message);
                                let _send_once_and_log =
                                    |message_type: MessageType, data: Vec<u8>| {
                                        let result = Connection::send_once_with(
                                            message_type,
                                            data,
                                            &command_sender,
                                        );
                                        if let Err(e) = result {
                                            println!("Error occurred : {:?}", e);
                                        }
                                    };

                                let send_response_and_log =
                                    |message_type: MessageType, data: Vec<u8>| {
                                        let result = Connection::send_response_with(
                                            message_type,
                                            data,
                                            &message.correlation_id,
                                            &command_sender,
                                        );
                                        if let Err(e) = result {
                                            println!("Error occurred : {:?}", e);
                                        }
                                    };
                                match message.message_type() {
                                    MessageType::NetworkConnect => {
                                        let connect_request: proto::ConnectionRequest =
                                            message.content.parse_into().unwrap();
                                        println!("connect request = {:?}", connect_request);
                                        let response = proto::ConnectionResponse {
                                            status: proto::connection_response::Status::Ok
                                                .into(),
                                            roles: vec![proto::connection_response::RoleEntry { role: proto::RoleType::Network.into(), auth_type: proto::connection_response::AuthorizationType::Trust.into() }],
                                        };
                                        send_response_and_log(
                                            MessageType::AuthorizationConnectionResponse,
                                            response.to_bytes(),
                                        );
                                    }
                                    MessageType::PingRequest => {
                                        let ping_request = message
                                            .content
                                            .parse_into::<proto::PingRequest>()
                                            .unwrap();
                                        println!("ping request = {:?}", ping_request);
                                        send_response_and_log(
                                            MessageType::PingResponse,
                                            proto::PingResponse::default().to_bytes(),
                                        );
                                    }
                                    MessageType::AuthorizationTrustRequest => {
                                        let response = proto::AuthorizationTrustResponse {
                                            roles: vec![proto::RoleType::Network.into()],
                                        };
                                        println!("Sending response {:?}", response);
                                        send_response_and_log(
                                            MessageType::AuthorizationTrustResponse,
                                            response.to_bytes(),
                                        );
                                    }
                                    MessageType::GossipGetPeersResponse => {
                                        let response = message.content.parse_into::<proto::GetPeersResponse>().unwrap();
                                        println!("Peers response = {:?}", response);
                                        
                                    }
                                    msg => println!("unhandled message type {:?}", msg),
                                }
                            }
                        }
                        Err(e) => {
                            println!("Failed to parse message: {:?}", e);
                        }
                    }
                }
            }
            Some(Err(e)) => {
                println!("tmq error: {}", e);
            }
            None => {
                println!("No message");
            }
        }
    }

    async fn handle_command(
        command: Option<Command>,
        sock: &mut Dealer,
        response_queue: &mut ResponseQueue,
    ) {
        match command {
            Some(Command::Send {
                message,
                response_demand,
            }) => {
                match sock.send(message).await {
                    Ok(()) => {}
                    Err(e) => {
                        println!("Error sending message: {:?}", e)
                    }
                }
                if let Some(ResponseDemand { correlation_id, tx }) = response_demand {
                    response_queue.insert(correlation_id, tx);
                }
            }
            None => {}
        }
    }

    pub fn send_request_with(
        message_type: MessageType,
        data: Vec<u8>,
        sender: &mpsc::UnboundedSender<Command>,
    ) -> Result<ResponseFuture> {
        let correlation_id = correlation_id();

        let message = proto::Message {
            message_type: message_type.into(),
            correlation_id: correlation_id.clone(),
            content: data,
        };

        println!("sending message {:?}", message);

        let message_bytes = message.to_bytes();
        let message = vec![&message_bytes].into();

        let (tx, rx) = oneshot::channel();

        sender.send(Command::Send {
            message,
            response_demand: Some(ResponseDemand { correlation_id, tx }),
        })?;

        Ok(ResponseFuture { rx })
    }

    pub fn send_once_with(
        message_type: MessageType,
        data: Vec<u8>,
        sender: &mpsc::UnboundedSender<Command>,
    ) -> Result<()> {
        let message = proto::Message {
            message_type: message_type.into(),
            correlation_id: correlation_id(),
            content: data,
        };

        let message_bytes = message.to_bytes();
        let message = vec![&message_bytes];

        sender
            .send(Command::Send {
                message: message.into(),
                response_demand: None,
            })
            .map_err(Into::into)
    }

    pub fn send_response_with(
        message_type: MessageType,
        data: Vec<u8>,
        correlation_id: &str,
        sender: &mpsc::UnboundedSender<Command>,
    ) -> Result<()> {
        let message = proto::Message {
            message_type: message_type.into(),
            correlation_id: correlation_id.to_owned(),
            content: data,
        };

        let message_bytes = message.to_bytes();
        let message = vec![&message_bytes];

        sender
            .send(Command::Send {
                message: message.into(),
                response_demand: None,
            })
            .map_err(Into::into)
    }
}

fn correlation_id() -> String {
    format!("{:x}", Uuid::new_v4().to_simple())
}

impl Connection {
    pub fn send_once(&self, message_type: MessageType, data: Vec<u8>) -> Result<()> {
        Connection::send_once_with(message_type, data, &self.tx)
    }

    pub fn send_request(&self, message_type: MessageType, data: Vec<u8>) -> Result<ResponseFuture> {
        Connection::send_request_with(message_type, data, &self.tx)
    }
}

#[derive(Debug)]
pub enum NetworkCommand {}

#[derive(Debug)]
pub enum NetworkEvent {}

impl Network {
    pub fn new(secret_key: SecretKey) -> Result<Self> {
        let context = tmq::Context::new();
        let recv_sock = tmq::router(&context).bind("tcp://0.0.0.0:8801")?;
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (update_tx, update_rx) = mpsc::unbounded_channel();

        let (stop_tx, stop_rx) = broadcast::channel(10);

        let my_stop_tx = stop_tx.clone();
        let _task = tokio::spawn(async move {
            let mut recv_sock = recv_sock;
            let mut command_rx = command_rx;
            let mut update_rx = update_rx;
            let mut stop_rx = stop_rx;

            loop {
                tokio::select! {
                    incoming = recv_sock.next() => {
                        println!("Got message on router: {:?}", incoming);
                    }
                    command = command_rx.recv() => {
                        println!("Got command: {:?}", command);
                    }
                    event = update_rx.recv() => {
                        println!("Got event: {:?}", event);
                    }
                    _ = tokio::signal::ctrl_c() => {
                        println!("Got ctrl c");
                        my_stop_tx.send(()).unwrap();
                    }
                    _ = stop_rx.recv() => {
                        println!("Stopping");
                        break;
                    }
                }
            }
        });

        Ok(Self {
            context,
            secret_key,
            connections: HashMap::default(),
            command_tx,
            update_tx,
            stop_tx,
        })
    }

    pub fn make_sender(&self) -> mpsc::UnboundedSender<NetworkCommand> {
        self.command_tx.clone()
    }

    pub fn send_once(
        &self,
        message_type: MessageType,
        data: impl Into<Vec<u8>>,
        connection_id: ConnectionId,
    ) -> Result<()> {
        if let Some(conn) = self.connections.get(&connection_id) {
            Ok(conn.send_once(message_type, data.into())?)
        } else {
            Err(eyre!("unknown connection id : {:?}", connection_id))
        }
    }

    pub fn send_request(
        &self,
        message_type: MessageType,
        data: impl Into<Vec<u8>>,
        connection_id: ConnectionId,
    ) -> Result<ResponseFuture> {
        if let Some(conn) = self.connections.get(&connection_id) {
            Ok(conn.send_request(message_type, data.into())?)
        } else {
            Err(eyre!("unknown connection id : {:?}", connection_id))
        }
    }

    pub fn send_proto<M: prost::Message + Default>(
        &self,
        message_type: MessageType,
        message: &M,
        connection_id: ConnectionId,
    ) -> Result<()> {
        self.send_once(message_type, message.to_bytes(), connection_id)
    }

    pub async fn connect_to(&mut self, endpoint: impl AsRef<str>) -> Result<ConnectionId> {
        let connection = Connection::create(
            endpoint,
            &self.context,
            self.command_tx.clone(),
            self.update_tx.clone(),
            self.stop_tx.subscribe(),
        )?;
        let connect_request = proto::ConnectionRequest {
            endpoint: PUBLIC_ENDPOINT.into(),
        };

        let reply =
            connection.send_request(MessageType::NetworkConnect, connect_request.to_bytes())?;

        match reply.await {
            Ok(reply) => {
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
                        .send_request(
                            MessageType::AuthorizationTrustRequest,
                            trust_request.to_bytes(),
                        )?
                        .await;

                    match response {
                        Ok(response) => {
                            ensure_message_type!(
                                response.message_type(),
                                MessageType::AuthorizationTrustResponse
                            );
                            println!("Trust request response: {:?}", response);
                        }
                        Err(e) => {
                            println!("error: {:?}", e)
                        }
                    }
                }
                if !challenge_roles.is_empty() {
                    let challenge_request = proto::AuthorizationChallengeRequest::default();
                    let response = connection
                        .send_request(
                            MessageType::AuthorizationChallengeRequest,
                            challenge_request.to_bytes(),
                        )?
                        .await?;

                    ensure_message_type!(
                        response.message_type(),
                        MessageType::AuthorizationChallengeResponse
                    );
                    println!("Challenge request response: {:?}", response);
                }
            }
            Err(e) => return Err(eyre!("Got no response to connection request: {}", e)),
        }

        let id = connection.id;
        self.connections.insert(id, connection);
        Ok(id)
    }

    pub fn stop_tx(&self) -> broadcast::Sender<()> {
        self.stop_tx.clone()
    }

    pub fn stop_rx(&self) -> broadcast::Receiver<()> {
        self.stop_tx.subscribe()
    }
}
