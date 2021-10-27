use crate::cli::CliOptions;
use crate::ext::{BufExt, FutureExt, MessageExt, ResultExt};
use crate::proto::{self, message::MessageType};
use crate::CreditcoinNode;
use core::fmt;
use dashmap::DashMap;
use futures::{Future, FutureExt as _, SinkExt, StreamExt};
use internment::Intern;
use secp256k1::SecretKey;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
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
    connections: Arc<DashMap<ConnectionId, Connection>>,
    command_tx: mpsc::UnboundedSender<NetworkCommand>,
    update_tx: mpsc::UnboundedSender<NetworkEvent>,
    stop_tx: broadcast::Sender<()>,
    update_rx: Option<mpsc::UnboundedReceiver<NetworkEvent>>,
    public_endpoint: String,
}

pub struct Connection {
    #[allow(dead_code)]
    endpoint: String,
    id: ConnectionId,
    tx: mpsc::UnboundedSender<Command>,
    #[allow(dead_code)]
    task: Option<JoinHandle<()>>,
}

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

        let this_endpoint = Intern::new(endpoint.clone());

        let task = tokio::spawn(async move {
            let mut response_queue: ResponseQueue = HashMap::default();
            let mut queue = queue;
            let mut sock = sock;
            let command_tx = command_tx;
            let update_tx = update_tx;
            let mut stop_rx = stop_rx;
            let internal_tx = internal_tx;
            let id = id.clone();
            let endpoint = this_endpoint;

            loop {
                tokio::select! {
                    message = queue.recv() => {
                        Self::handle_command(message, &mut sock, &mut response_queue).await;
                    }
                    incoming = sock.next() => {
                        log::trace!("incoming {:?}", incoming);
                        Self::handle_message(incoming, &mut response_queue, &internal_tx, &command_tx, &update_tx, endpoint, id).await;
                    }
                    _ = stop_rx.recv() => {
                        log::debug!("stopping connection to {}", endpoint);
                        drop(update_tx);
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
        _network_command_sender: &mpsc::UnboundedSender<NetworkCommand>,
        network_update_sender: &mpsc::UnboundedSender<NetworkEvent>,
        this_endpoint: Intern<String>,
        _this_id: ConnectionId,
    ) {
        match message {
            Some(Ok(mut message)) => {
                if message.len() > 1 {
                    log::warn!("Message longer than expected");
                }
                if let Some(frame) = message.pop_back() {
                    let message = proto::Message::try_parse(&*frame);
                    match message {
                        Ok(message) => {
                            if let Some(tx) = response_queue.remove(&message.correlation_id) {
                                if let Err(e) = tx.send(message) {
                                    log::error!("Failed to send response over channel: {:?}", e)
                                }
                            } else {
                                log::trace!("Non-response message! {:?}", message);
                                let _send_once_and_log =
                                    |message_type: MessageType, data: Vec<u8>| {
                                        Connection::send_once_with(
                                            message_type,
                                            data,
                                            &command_sender,
                                        )
                                        .log_err();
                                    };

                                let send_response_and_log =
                                    |message_type: MessageType, data: Vec<u8>| {
                                        Connection::send_response_with(
                                            message_type,
                                            data,
                                            &message.correlation_id,
                                            &command_sender,
                                        )
                                        .log_err();
                                    };
                                match message.message_type() {
                                    MessageType::NetworkConnect => {
                                        let connect_request: proto::ConnectionRequest =
                                            message.content.parse_into().unwrap();
                                        log::debug!("connect request = {:?}", connect_request);
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
                                        log::trace!("ping request = {:?}", ping_request);
                                        send_response_and_log(
                                            MessageType::PingResponse,
                                            proto::PingResponse::default().to_bytes(),
                                        );
                                    }
                                    MessageType::AuthorizationTrustRequest => {
                                        let response = proto::AuthorizationTrustResponse {
                                            roles: vec![proto::RoleType::Network.into()],
                                        };
                                        log::trace!("Sending response {:?}", response);
                                        send_response_and_log(
                                            MessageType::AuthorizationTrustResponse,
                                            response.to_bytes(),
                                        );
                                    }
                                    MessageType::GossipGetPeersResponse => {
                                        let response = message
                                            .content
                                            .parse_into::<proto::GetPeersResponse>()
                                            .unwrap();
                                        let peers = response.peer_endpoints;
                                        let node = crate::CreditcoinNode {
                                            endpoint: this_endpoint.to_owned(),
                                        };
                                        network_update_sender
                                            .send(NetworkEvent::DiscoveredPeers { node, peers })
                                            .log_err();
                                    }
                                    msg => log::warn!("unhandled message type {:?}", msg),
                                }
                            }
                        }
                        Err(e) => {
                            log::error!("Failed to parse message: {:?}", e);
                        }
                    }
                }
            }
            Some(Err(e)) => {
                log::error!("tmq error: {}", e);
            }
            None => {
                log::warn!("No message");
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
                        log::error!("Error sending message: {:?}", e)
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

        log::trace!("sending message {:?}", message);

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
pub enum NetworkCommand {
    ConnectToNode { endpoint: String },
    RequestPeers {},
}

#[derive(Debug)]
pub enum NetworkEvent {
    DiscoveredPeers {
        node: CreditcoinNode,
        peers: Vec<String>,
    },
}

impl Network {
    pub fn new(
        secret_key: SecretKey,
        bind: impl AsRef<str>,
        endpoint: impl AsRef<str>,
    ) -> Result<Self> {
        let context = tmq::Context::new();
        let recv_sock = tmq::router(&context).bind(bind.as_ref())?;
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (update_tx, update_rx) = mpsc::unbounded_channel();

        let (stop_tx, stop_rx) = broadcast::channel(10);

        let connections = Arc::new(DashMap::new());

        let connections_task = Arc::clone(&connections);

        let my_stop_tx = stop_tx.clone();
        let _task = tokio::spawn(async move {
            let mut recv_sock = recv_sock;
            let mut command_rx = command_rx;
            let mut stop_rx = stop_rx;
            let _connections_task = connections_task;
            loop {
                tokio::select! {
                    incoming = recv_sock.next() => {
                        log::debug!("Got message on router: {:?}", incoming);
                    }
                    command = command_rx.recv() => {
                        log::debug!("Got command: {:?}", command);
                    }
                    _ = tokio::signal::ctrl_c() => {
                        log::debug!("Got ctrl c");
                        my_stop_tx.send(()).unwrap();
                    }
                    _ = stop_rx.recv() => {
                        log::debug!("Stopping");
                        break;
                    }
                }
            }
        });

        Ok(Self {
            context,
            secret_key,
            connections,
            command_tx,
            update_tx,
            stop_tx,
            public_endpoint: endpoint.as_ref().to_owned(),
            update_rx: Some(update_rx),
        })
    }

    pub async fn with_options(secret_key: SecretKey, opts: &CliOptions) -> Result<Self> {
        let network = Self::new(secret_key, opts.bind_address(), opts.endpoint())?;

        for seed in &opts.seeds {
            network.connect_to(seed).await?;
        }

        Ok(network)
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

    pub async fn request_peers_of(&self, connection_id: ConnectionId) -> Result<()> {
        let response = tokio::time::timeout(
            Duration::from_secs(30),
            self.send_request(
                MessageType::GossipGetPeersRequest,
                proto::GetPeersRequest::default().to_bytes(),
                connection_id,
            )?,
        )
        .await??;

        log::trace!("RESPONSE = {:?}", response);

        let _ = response
            .content
            .parse_into::<proto::NetworkAcknowledgement>()?;

        log::trace!("Got ACK");

        Ok(())
    }

    pub async fn connect_to(&self, endpoint: impl AsRef<str>) -> Result<ConnectionId> {
        let connection = Connection::create(
            endpoint,
            &self.context,
            self.command_tx.clone(),
            self.update_tx.clone(),
            self.stop_tx.subscribe(),
        )?;
        let connect_request = proto::ConnectionRequest {
            endpoint: self.public_endpoint.clone(),
        };

        let reply =
            connection.send_request(MessageType::NetworkConnect, connect_request.to_bytes())?;

        match reply.timeout(Duration::from_secs(30)).await {
            Ok(Ok(reply)) => {
                ensure_message_type!(
                    reply.message_type(),
                    MessageType::AuthorizationConnectionResponse
                );
                log::trace!("{:?}", reply);
                let response = proto::ConnectionResponse::try_parse(&reply.content)?;
                log::trace!("{:?}", response);

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
                        .timeout(Duration::from_secs(10))
                        .await;

                    match response {
                        Ok(Ok(response)) => {
                            ensure_message_type!(
                                response.message_type(),
                                MessageType::AuthorizationTrustResponse
                            );
                            log::trace!("Trust request response: {:?}", response);
                        }
                        Ok(Err(e)) => {
                            log::error!("error occurred making authorization trust request: {}", e)
                        }
                        Err(e) => {
                            log::error!("timed out waiting for trust response: {:?}", e)
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
                    log::trace!("Challenge request response: {:?}", response);
                }
            }
            Err(e) => return Err(eyre!("Timed out while waiting for response: {}", e)),
            Ok(Err(e)) => return Err(eyre!("Got no response to connection request: {}", e)),
        }

        let id = connection.id;
        self.connections.insert(id, connection);
        Ok(id)
    }

    pub fn take_update_rx(&mut self) -> Option<mpsc::UnboundedReceiver<NetworkEvent>> {
        self.update_rx.take()
    }

    pub fn stop_tx(&self) -> broadcast::Sender<()> {
        self.stop_tx.clone()
    }

    pub fn stop_rx(&self) -> broadcast::Receiver<()> {
        self.stop_tx.subscribe()
    }

    pub fn command_tx(&self) -> mpsc::UnboundedSender<NetworkCommand> {
        self.command_tx.clone()
    }
}
