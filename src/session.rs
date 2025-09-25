use std::collections::HashMap;
use std::io;
use crate::connection::{Connection};
use crate::subscription::{AckMode, AckOrNack, Subscription};
use crate::frame::{Frame, Command, ToFrameBody};
use crate::frame::Transmission::{self, HeartBeat as TxHeartBeat, CompleteFrame};
use crate::header::{self, Header};
use crate::transaction::Transaction;
use crate::session_builder::SessionConfig;
use crate::message_builder::MessageBuilder;
use crate::subscription_builder::SubscriptionBuilder;
use crate::codec::Codec;

use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use futures::{Stream, StreamExt, SinkExt};
use std::task::{Context, Poll};
use std::pin::Pin;
const GRACE_PERIOD_MULTIPLIER: f32 = 2.0;

pub struct OutstandingReceipt {
    pub original_frame: Frame,
}

impl OutstandingReceipt {
    pub fn new(original_frame: Frame) -> Self {
        OutstandingReceipt {
            original_frame
        }
    }
}

pub struct GenerateReceipt;

pub struct ReceiptRequest {
    pub id: String,
}

impl ReceiptRequest {
    pub fn new(id: String) -> Self {
        ReceiptRequest {
            id,
        }
    }
}

pub struct SessionState {
    next_transaction_id: u32,
    next_subscription_id: u32,
    next_receipt_id: u32,
    pub rx_heartbeat_ms: Option<u32>,
    pub tx_heartbeat_ms: Option<u32>,
    pub subscriptions: HashMap<String, Subscription>,
    pub outstanding_receipts: HashMap<String, OutstandingReceipt>,
}

impl SessionState {
    pub fn new() -> SessionState {
        SessionState {
            next_transaction_id: 0,
            next_subscription_id: 0,
            next_receipt_id: 0,
            rx_heartbeat_ms: None,
            tx_heartbeat_ms: None,
            subscriptions: HashMap::new(),
            outstanding_receipts: HashMap::new(),
        }
    }
}

#[derive(Debug)]
pub enum DisconnectionReason {
    RecvFailed(io::Error),
    ConnectFailed(io::Error),
    SendFailed(io::Error),
    ClosedByOtherSide,
    HeartbeatTimeout,
    Requested,
}

#[derive(Debug)]
pub enum SessionEvent {
    Connected,
    ErrorFrame(Frame),
    Receipt {
        id: String,
        original: Frame,
        receipt: Frame,
    },
    Message {
        destination: String,
        ack_mode: AckMode,
        frame: Frame,
    },
    SubscriptionlessFrame(Frame),
    UnknownFrame(Frame),
    Disconnected(DisconnectionReason),
}

pub enum StreamState {
    Connected(Framed<TcpStream, Codec>),
    Connecting(Pin<Box<dyn std::future::Future<Output = io::Result<TcpStream>> + Send>>),
    Failed,
}

pub struct Session {
    config: SessionConfig,
    pub state: SessionState,
    stream: StreamState,
    events: Vec<SessionEvent>,
}

impl Session {
    pub async fn new(config: SessionConfig) -> io::Result<Session> {
        let addr = format!("{}:{}", config.host, config.port);
        let stream = TcpStream::connect(&addr).await?;
        let framed = Framed::new(stream, Codec);
        
        let mut session = Session {
            config,
            state: SessionState::new(),
            stream: StreamState::Connected(framed),
            events: vec![],
        };
        
        session.on_stream_ready().await?;
        Ok(session)
    }

    // Public API
    pub async fn send_frame(&mut self, frame: Frame) -> io::Result<()> {
        self.send(Transmission::CompleteFrame(frame)).await
    }

    pub fn message<'builder, T: ToFrameBody>(&'builder mut self,
                                             destination: &str,
                                             body_convertible: T)
                                             -> MessageBuilder<'builder> {
        let send_frame = Frame::send(destination, body_convertible.to_frame_body());
        MessageBuilder::new(self, send_frame)
    }

    pub fn subscription<'builder>(&'builder mut self,
                                  destination: &str)
                                  -> SubscriptionBuilder<'builder> {
        SubscriptionBuilder::new(self, destination.to_owned())
    }

    pub fn begin_transaction<'b>(&'b mut self) -> Transaction<'b> {
        Transaction::new(self)
    }

    pub async fn unsubscribe(&mut self, sub_id: &str) -> io::Result<()> {
        self.state.subscriptions.remove(sub_id);
        let unsubscribe_frame = Frame::unsubscribe(sub_id);
        self.send(CompleteFrame(unsubscribe_frame)).await
    }

    pub async fn disconnect(&mut self) -> io::Result<()> {
        self.send_frame(Frame::disconnect()).await
    }

    pub async fn reconnect(&mut self) -> io::Result<()> {
        log::info!("Reconnecting...");
        let addr = format!("{}:{}", self.config.host, self.config.port);
        let stream = TcpStream::connect(&addr).await?;
        let framed = Framed::new(stream, Codec);
        self.stream = StreamState::Connected(framed);
        self.on_stream_ready().await?;
        Ok(())
    }

    pub async fn acknowledge_frame(&mut self, frame: &Frame, which: AckOrNack) -> io::Result<()> {
        if let Some(header::Ack(ack_id)) = frame.headers.get_ack() {
            let ack_frame = match which {
                AckOrNack::Ack => Frame::ack(ack_id),
                AckOrNack::Nack => Frame::nack(ack_id),
            };
            self.send_frame(ack_frame).await?;
        }
        Ok(())
    }

    // Internal API helpers
    pub fn generate_transaction_id(&mut self) -> u32 {
        let id = self.state.next_transaction_id;
        self.state.next_transaction_id += 1;
        id
    }

    pub fn generate_subscription_id(&mut self) -> u32 {
        let id = self.state.next_subscription_id;
        self.state.next_subscription_id += 1;
        id
    }

    pub fn generate_receipt_id(&mut self) -> u32 {
        let id = self.state.next_receipt_id;
        self.state.next_receipt_id += 1;
        id
    }

    async fn send(&mut self, tx: Transmission) -> io::Result<()> {
        if let StreamState::Connected(ref mut stream) = self.stream {
            stream.send(tx).await?;
        } else {
            log::warn!("sending {:?} whilst disconnected", tx);
        }
        Ok(())
    }

    async fn on_stream_ready(&mut self) -> io::Result<()> {
        log::debug!("Stream ready!");
        
        // Add credentials to the header list if specified
        if let Some(ref credentials) = self.config.credentials.clone() {
            log::debug!("Using provided credentials: login '{}', passcode '{}'",
                       credentials.login, credentials.passcode);
            self.config.headers.push(Header::new("login", &credentials.login));
            self.config.headers.push(Header::new("passcode", &credentials.passcode));
        } else {
            log::debug!("No credentials supplied.");
        }

        let crate::connection::HeartBeat(client_tx_ms, client_rx_ms) = self.config.heartbeat;
        let heart_beat_string = format!("{},{}", client_tx_ms, client_rx_ms);
        log::debug!("Using heartbeat: {},{}", client_tx_ms, client_rx_ms);
        self.config.headers.push(Header::new("heart-beat", &heart_beat_string));

        let connect_frame = Frame {
            command: Command::Connect,
            headers: self.config.headers.clone(),
            body: Vec::new(),
        };

        self.send_frame(connect_frame).await?;
        Ok(())
    }

    fn on_message(&mut self, frame: Frame) {
        let mut sub_data = None;
        if let Some(header::Subscription(sub_id)) = frame.headers.get_subscription() {
            if let Some(ref sub) = self.state.subscriptions.get(sub_id) {
                sub_data = Some((sub.destination.clone(), sub.ack_mode));
            }
        }
        
        if let Some((destination, ack_mode)) = sub_data {
            self.events.push(SessionEvent::Message {
                destination,
                ack_mode,
                frame,
            });
        } else {
            self.events.push(SessionEvent::SubscriptionlessFrame(frame));
        }
    }

    fn on_connected_frame_received(&mut self, connected_frame: Frame) -> io::Result<()> {
        // The Client's requested tx/rx HeartBeat timeouts
        let crate::connection::HeartBeat(client_tx_ms, client_rx_ms) = self.config.heartbeat;

        // The timeouts the server is willing to provide
        let (server_tx_ms, server_rx_ms) = match connected_frame.headers.get_heart_beat() {
            Some(header::HeartBeat(tx_ms, rx_ms)) => (tx_ms, rx_ms),
            _ => (0, 0),
        };

        let (agreed_upon_tx_ms, agreed_upon_rx_ms) = Connection::select_heartbeat(
            client_tx_ms, client_rx_ms, server_tx_ms, server_rx_ms);
        
        self.state.rx_heartbeat_ms = Some((agreed_upon_rx_ms as f32 * GRACE_PERIOD_MULTIPLIER) as u32);
        self.state.tx_heartbeat_ms = Some(agreed_upon_tx_ms);

        self.events.push(SessionEvent::Connected);
        Ok(())
    }

    fn handle_receipt(&mut self, frame: Frame) {
        let receipt_id = if let Some(header::ReceiptId(receipt_id)) = frame.headers.get_receipt_id() {
            Some(receipt_id.to_owned())
        } else {
            None
        };
        
        if let Some(receipt_id) = receipt_id {
            if receipt_id == "msg/disconnect" {
                self.events.push(SessionEvent::Disconnected(DisconnectionReason::Requested));
            }
            if let Some(entry) = self.state.outstanding_receipts.remove(&receipt_id) {
                let original_frame = entry.original_frame;
                self.events.push(SessionEvent::Receipt {
                    id: receipt_id,
                    original: original_frame,
                    receipt: frame,
                });
            }
        }
    }

    pub async fn next_event(&mut self) -> Option<SessionEvent> {
        loop {
            // First, check if we have any pending events
            if !self.events.is_empty() {
                return Some(self.events.remove(0));
            }

            // Then try to read from the stream
            match &mut self.stream {
                StreamState::Connected(ref mut stream) => {
                    match stream.next().await {
                        Some(Ok(transmission)) => {
                            match transmission {
                                TxHeartBeat => {
                                    log::debug!("Received heartbeat.");
                                    // Continue loop to check for more events
                                }
                                CompleteFrame(frame) => {
                                    log::debug!("Received frame: {:?}", frame);
                                    match frame.command {
                                        Command::Error => self.events.push(SessionEvent::ErrorFrame(frame)),
                                        Command::Receipt => self.handle_receipt(frame),
                                        Command::Connected => {
                                            if let Err(e) = self.on_connected_frame_received(frame) {
                                                self.events.push(SessionEvent::Disconnected(DisconnectionReason::RecvFailed(e)));
                                            }
                                        }
                                        Command::Message => self.on_message(frame),
                                        _ => self.events.push(SessionEvent::UnknownFrame(frame)),
                                    }
                                }
                            }
                        }
                        Some(Err(e)) => {
                            self.events.push(SessionEvent::Disconnected(DisconnectionReason::RecvFailed(e)));
                        }
                        _ => {
                            self.events.push(SessionEvent::Disconnected(DisconnectionReason::ClosedByOtherSide));
                        }
                    }
                }
                StreamState::Connecting(_) => {
                    // For simplicity, we'll return None if still connecting
                    return None;
                }
                StreamState::Failed => {
                    return None;
                }
            }
        }
    }
}

impl Stream for Session {
    type Item = SessionEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // First, check if we have any buffered events
            if !self.events.is_empty() {
                return Poll::Ready(Some(self.events.remove(0)));
            }

            // Poll the underlying stream for new data
            match &mut self.stream {
                StreamState::Connected(ref mut stream) => {
                    match Pin::new(stream).poll_next(cx) {
                        Poll::Ready(Some(Ok(transmission))) => {
                            // Process the transmission and generate events
                            match transmission {
                                TxHeartBeat => {
                                    log::debug!("Received heartbeat.");
                                    // Continue loop to check for more data
                                }
                                CompleteFrame(frame) => {
                                    log::debug!("Received frame: {:?}", frame);
                                    match frame.command {
                                        Command::Error => {
                                            self.events.push(SessionEvent::ErrorFrame(frame));
                                        }
                                        Command::Receipt => {
                                            self.handle_receipt(frame);
                                        }
                                        Command::Connected => {
                                            if let Err(e) = self.on_connected_frame_received(frame) {
                                                self.events.push(SessionEvent::Disconnected(
                                                    DisconnectionReason::RecvFailed(e)
                                                ));
                                            }
                                        }
                                        Command::Message => {
                                            self.on_message(frame);
                                        }
                                        _ => {
                                            self.events.push(SessionEvent::UnknownFrame(frame));
                                        }
                                    }
                                }
                            }
                            // Continue loop to return any generated events
                        }
                        Poll::Ready(Some(Err(e))) => {
                            self.events.push(SessionEvent::Disconnected(
                                DisconnectionReason::RecvFailed(e)
                            ));
                            // Continue loop to return the disconnection event
                        }
                        Poll::Ready(_) => {
                            // Stream ended
                            self.events.push(SessionEvent::Disconnected(
                                DisconnectionReason::ClosedByOtherSide
                            ));
                            // Continue loop to return the disconnection event
                        }
                        Poll::Pending => {
                            // No data available, return pending
                            return Poll::Pending;
                        }
                    }
                }
                StreamState::Connecting(_) => {
                    // Still connecting, no events yet
                    return Poll::Pending;
                }
                StreamState::Failed => {
                    // Stream has failed, no more events
                    return Poll::Ready(None);
                }
            }
        }
    }
}
