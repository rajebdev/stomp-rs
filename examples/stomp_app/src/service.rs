use anyhow::{Context, Result};
use chrono::Utc;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::task::JoinSet;
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

use stomp::connection::{Credentials, HeartBeat};
use stomp::header::Header;
use stomp::option_setter::OptionSetter;
use stomp::session::{Session, SessionEvent};
use stomp::session_builder::SessionBuilder;
use stomp::subscription::{AckMode, AckOrNack};

use crate::config::Config;

// Option setters for the session builder (following reference implementation)
struct WithHeader(Header);
struct WithHeartBeat(HeartBeat);
struct WithCredentials<'a>(Credentials<'a>);

impl OptionSetter<SessionBuilder> for WithHeader {
    fn set_option(self, mut builder: SessionBuilder) -> SessionBuilder {
        builder.config.headers.push(self.0);
        builder
    }
}

impl OptionSetter<SessionBuilder> for WithHeartBeat {
    fn set_option(self, mut builder: SessionBuilder) -> SessionBuilder {
        builder.config.heartbeat = self.0;
        builder
    }
}

impl<'a> OptionSetter<SessionBuilder> for WithCredentials<'a> {
    fn set_option(self, mut builder: SessionBuilder) -> SessionBuilder {
        builder.config.credentials = Some(stomp::connection::OwnedCredentials::from(self.0));
        builder
    }
}

/// Core STOMP service for handling message operations
pub struct StompService {
    config: Config,
    session: Option<Session>,
    /// Tracks whether the connection is healthy
    is_connected: Arc<AtomicBool>,
    /// Shutdown signal for graceful cleanup
    shutdown_tx: Option<broadcast::Sender<()>>,
    shutdown_rx: Option<broadcast::Receiver<()>>,
    /// Track subscription destinations for reconnection
    active_subscriptions: Arc<std::sync::Mutex<Vec<String>>>,
}

impl StompService {
    /// Create a new STOMP service instance
    pub async fn new(config: Config) -> Result<Self> {
        info!("🚀 Initializing STOMP service: {}", config.service.name);

        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        Ok(Self {
            config,
            session: None,
            is_connected: Arc::new(AtomicBool::new(false)),
            shutdown_tx: Some(shutdown_tx),
            shutdown_rx: Some(shutdown_rx),
            active_subscriptions: Arc::new(std::sync::Mutex::new(Vec::new())),
        })
    }

    /// Connect to the STOMP broker using the simplified pattern from reference
    pub async fn connect(&mut self) -> Result<()> {
        let (send_heartbeat, recv_heartbeat) = self.config.get_heartbeat_ms();
        info!(
            "🔗 Connecting to STOMP broker at {}:{}",
            self.config.broker.host, self.config.broker.port
        );

        // Build session using the reference pattern with option setters
        let mut session_builder =
            SessionBuilder::new(&self.config.broker.host, self.config.broker.port)
                .with(WithHeartBeat(HeartBeat(send_heartbeat, recv_heartbeat)));

        // Add credentials if available
        if let Some((username, password)) = self.config.get_credentials() {
            session_builder =
                session_builder.with(WithCredentials(Credentials(&username, &password)));
            info!("🔐 Using credentials: {}", username);
        }

        // Add client ID header
        let client_id = format!("{}-{}", self.config.service.name, Uuid::new_v4());
        session_builder =
            session_builder.with(WithHeader(Header::new("custom-client-id", &client_id)));

        // Add custom headers
        for (key, value) in &self.config.broker.headers {
            session_builder = session_builder.with(WithHeader(Header::new(key, value)));
        }

        let session = session_builder
            .start()
            .await
            .with_context(|| "Failed to establish STOMP connection")?;

        self.session = Some(session);
        self.is_connected.store(true, Ordering::Relaxed);
        info!(
            "✅ Successfully connected to STOMP broker with client-id: {}",
            client_id
        );

        Ok(())
    }

    /// Send message to a topic (simplified pattern following reference)
    pub async fn send_topic(
        &mut self,
        topic_name: &str,
        payload: &str,
        _headers: HashMap<String, String>,
    ) -> Result<()> {
        // Ensure we have an active session
        if self.session.is_none() {
            self.connect().await?;
        }

        let session = self
            .session
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("No active STOMP session"))?;

        // Build destination path - try config first, then fallback to direct path
        let destination = if let Some(topic_config) = self.config.get_topic_config(topic_name) {
            topic_config.path.clone()
        } else if topic_name.starts_with("/topic/") {
            topic_name.to_string()
        } else {
            format!("/topic/{}", topic_name)
        };

        info!("📤 Sending message to topic: {}", destination);

        // Send message using new comprehensive header support API with fluent chaining
        let _timestamp = Utc::now().to_rfc3339();

        let message_builder = session.message(&destination, payload);
        // .with_content_type("text/plain") // Set default content type
        // .add_header("message-type", "topic")
        // .add_header("timestamp", &timestamp);

        // Add custom headers using multiple approaches for demonstration
        // if !headers.is_empty() {
        //     // Method 1: Individual headers (most readable)
        //     for (key, value) in &headers {
        //         message_builder = message_builder.add_header(key, value);
        //     }
        // }

        // Send message and handle result with match
        match message_builder.send().await {
            Ok(_) => {
                info!("✅ Message sent successfully to topic: {}", destination);
                Ok(())
            }
            Err(e) => {
                error!("❌ Failed to send message to topic {}: {}", destination, e);
                Err(anyhow::anyhow!("Failed to send message to topic: {}", e))
            }
        }
    }

    /// Send message to a queue (simplified pattern following reference)
    pub async fn send_queue(
        &mut self,
        queue_name: &str,
        payload: &str,
        _headers: HashMap<String, String>,
    ) -> Result<()> {
        // Ensure we have an active session
        if self.session.is_none() {
            self.connect().await?;
        }

        let session = self
            .session
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("No active STOMP session"))?;

        // Build destination path - try config first, then fallback to direct path
        let destination = if let Some(queue_config) = self.config.get_queue_config(queue_name) {
            queue_config.path.clone()
        } else if queue_name.starts_with("/queue/") {
            queue_name.to_string()
        } else {
            format!("/queue/{}", queue_name)
        };

        info!("📤 Sending message to queue: {}", destination);

        // Send message using new comprehensive header support API with advanced features
        let _timestamp = Utc::now().to_rfc3339();

        let message_builder = session.message(&destination, payload);
        // .with_content_type("text/plain") // Set default content type
        // .add_header("message-type", "queue")
        // .add_header("timestamp", &timestamp);

        // Add custom headers with enhanced functionality
        // if !headers.is_empty() {
        //     // Check if we have priority override in custom headers
        //     if let Some(priority_str) = headers.get("priority") {
        //         if let Ok(priority) = priority_str.parse::<u8>() {
        //             message_builder = message_builder.with_priority(priority);
        //         }
        //     }

        //     // Check if we have content-type override
        //     if let Some(content_type) = headers.get("content-type") {
        //         message_builder = message_builder.with_content_type(content_type);
        //     }

        //     // Add remaining custom headers (excluding ones we already processed)
        //     for (key, value) in &headers {
        //         if key != "priority" && key != "content-type" {
        //             message_builder = message_builder.add_header(key, value);
        //         }
        //     }
        // }

        // Send message and handle result with match
        match message_builder.send().await {
            Ok(_) => {
                info!("✅ Message sent successfully to queue: {}", destination);
                Ok(())
            }
            Err(e) => {
                error!("❌ Failed to send message to queue {}: {}", destination, e);
                Err(anyhow::anyhow!("Failed to send message to queue: {}", e))
            }
        }
    }

    /// Receive messages from a topic with multiple workers based on configuration
    pub async fn receive_topic<F>(&mut self, topic_name: &str, handler: F) -> Result<()>
    where
        F: Fn(String) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
            + Send
            + Sync
            + Clone
            + 'static,
    {
        let worker_count = self.config.get_topic_workers(topic_name);
        info!(
            "📥 Starting {} workers for topic: {}",
            worker_count, topic_name
        );

        if worker_count == 1 {
            // Single worker - use direct implementation
            return self.receive_topic_single_worker(topic_name, handler).await;
        }

        // Multiple workers - spawn them as separate tasks
        let mut worker_tasks = JoinSet::new();

        for worker_id in 0..worker_count {
            let config_clone = self.config.clone();
            let topic_name = topic_name.to_string();
            let handler_clone = handler.clone();

            worker_tasks.spawn(async move {
                let worker_name = format!("{}#{}", topic_name, worker_id + 1);
                info!("🔄 Starting topic worker: {}", worker_name);

                // Each worker gets its own service instance
                let mut worker_service = match StompService::new(config_clone).await {
                    Ok(service) => service,
                    Err(e) => {
                        error!("❌ Failed to create worker service {}: {}", worker_name, e);
                        return Err(e);
                    }
                };

                // Start the worker
                let result = worker_service
                    .receive_topic_single_worker(&topic_name, handler_clone)
                    .await;

                match &result {
                    Ok(()) => info!("✅ Topic worker {} finished successfully", worker_name),
                    Err(e) => error!("❌ Topic worker {} failed: {}", worker_name, e),
                }

                result
            });
        }

        info!(
            "✅ All {} topic workers spawned for: {}",
            worker_count, topic_name
        );

        // Wait for any worker to complete (or fail)
        while let Some(result) = worker_tasks.join_next().await {
            match result {
                Ok(Ok(())) => {
                    info!("🔄 Topic worker completed successfully");
                    // Continue waiting for other workers
                }
                Ok(Err(e)) => {
                    warn!("⚠️ Topic worker failed: {}", e);
                    // For now, continue with other workers
                    // In production, you might want different error handling
                }
                Err(e) => {
                    error!("❌ Topic worker join error: {}", e);
                }
            }
        }

        Ok(())
    }

    /// Receive messages from a topic with a single worker (internal implementation)
    async fn receive_topic_single_worker<F>(&mut self, topic_name: &str, handler: F) -> Result<()>
    where
        F: Fn(String) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
            + Send
            + Sync
            + 'static,
    {
        // Try to get topic configuration, fallback to direct path
        let topic_path = if let Some(topic_config) = self.config.get_topic_config(topic_name) {
            topic_config.path.clone()
        } else if topic_name.starts_with("/topic/") {
            topic_name.to_string()
        } else {
            format!("/topic/{}", topic_name)
        };

        info!("📥 Starting topic worker for: {}", topic_path);

        // Main reconnection loop
        loop {
            // Check if we need to connect/reconnect
            if !self.is_healthy() {
                match self.reconnect().await {
                    Ok(()) => {
                        info!(
                            "✅ Successfully connected/reconnected to topic: {}",
                            topic_path
                        );
                    }
                    Err(e) => {
                        error!(
                            "❌ Failed to connect/reconnect to topic {}: {}",
                            topic_path, e
                        );
                        return Err(e);
                    }
                }
            }

            // Ensure we have a session
            if self.session.is_none() {
                self.connect().await?;
            }

            let mut session = self
                .session
                .take()
                .ok_or_else(|| anyhow::anyhow!("No active STOMP session"))?;

            // Add this destination to active subscriptions for reconnection
            {
                let mut subs = self.active_subscriptions.lock().unwrap();
                if !subs.contains(&topic_path) {
                    subs.push(topic_path.clone());
                }
            }

            let mut connected = false;
            let mut should_reconnect = false;

            while let Some(event) = session.next_event().await {
                match event {
                    SessionEvent::Connected => {
                        info!("✅ Connected to STOMP broker for topic subscription");

                        let subscription_id = session
                            .subscription(&topic_path)
                            .with(AckMode::ClientIndividual)
                            .start()
                            .await?;

                        info!(
                            "📬 Subscribed to topic: {} (ID: {})",
                            topic_path, subscription_id
                        );
                        connected = true;
                    }

                    SessionEvent::Message {
                        destination, frame, ..
                    } => {
                        if !connected {
                            continue;
                        }

                        let body_str = std::str::from_utf8(&frame.body)
                            .unwrap_or("<non-UTF8>")
                            .to_string();

                        let message_id = frame
                            .headers
                            .headers
                            .iter()
                            .find(|h| h.0 == "message-id")
                            .map(|h| h.1.as_str())
                            .unwrap_or("unknown");

                        info!(
                            "📨 Received topic message from {}: {} (ID: {})",
                            destination,
                            body_str.chars().take(50).collect::<String>(),
                            message_id
                        );

                        // Process message with handler
                        match handler(body_str).await {
                            Ok(()) => {
                                trace!("✅ Topic message processed successfully");

                                // Acknowledge the message
                                if let Err(e) =
                                    session.acknowledge_frame(&frame, AckOrNack::Ack).await
                                {
                                    error!("❌ Failed to acknowledge topic message: {}", e);
                                } else {
                                    trace!("✓ Topic message acknowledged");
                                }
                            }
                            Err(e) => {
                                error!("❌ Topic message processing failed: {}", e);

                                // Negative acknowledge (NACK) the message
                                if let Err(nack_err) =
                                    session.acknowledge_frame(&frame, AckOrNack::Nack).await
                                {
                                    error!("❌ Failed to NACK topic message: {}", nack_err);
                                }
                            }
                        }
                    }

                    SessionEvent::Receipt { id, .. } => {
                        debug!("📄 Received receipt: {}", id);
                    }

                    SessionEvent::ErrorFrame(frame) => {
                        error!("❌ Error frame received: {:?}", frame);
                        self.mark_unhealthy();
                        should_reconnect = true;
                        break;
                    }

                    SessionEvent::Disconnected(reason) => {
                        warn!("🔌 Session disconnected: {:?}", reason);
                        self.mark_unhealthy();
                        should_reconnect = true;
                        break;
                    }

                    _ => {
                        // Ignore other events
                    }
                }
            }

            // Store the session back if we're not reconnecting
            if !should_reconnect {
                self.session = Some(session);
                break;
            } else {
                info!(
                    "🔄 Connection lost for topic {}, will attempt reconnection...",
                    topic_path
                );
                // Session will be recreated on next iteration
                sleep(Duration::from_millis(1000)).await; // Brief pause before reconnecting
            }
        }

        Ok(())
    }

    /// Receive messages from a queue with multiple workers based on configuration
    pub async fn receive_queue<F>(&mut self, queue_name: &str, handler: F) -> Result<()>
    where
        F: Fn(String) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
            + Send
            + Sync
            + Clone
            + 'static,
    {
        let worker_count = self.config.get_queue_workers(queue_name);
        info!(
            "📥 Starting {} workers for queue: {}",
            worker_count, queue_name
        );

        if worker_count == 1 {
            // Single worker - use direct implementation
            return self.receive_queue_single_worker(queue_name, handler).await;
        }

        // Multiple workers - spawn them as separate tasks
        let mut worker_tasks = JoinSet::new();

        for worker_id in 0..worker_count {
            let config_clone = self.config.clone();
            let queue_name = queue_name.to_string();
            let handler_clone = handler.clone();

            worker_tasks.spawn(async move {
                let worker_name = format!("{}#{}", queue_name, worker_id + 1);
                info!("🔄 Starting queue worker: {}", worker_name);

                // Each worker gets its own service instance
                let mut worker_service = match StompService::new(config_clone).await {
                    Ok(service) => service,
                    Err(e) => {
                        error!("❌ Failed to create worker service {}: {}", worker_name, e);
                        return Err(e);
                    }
                };

                // Start the worker
                let result = worker_service
                    .receive_queue_single_worker(&queue_name, handler_clone)
                    .await;

                match &result {
                    Ok(()) => info!("✅ Queue worker {} finished successfully", worker_name),
                    Err(e) => error!("❌ Queue worker {} failed: {}", worker_name, e),
                }

                result
            });
        }

        info!(
            "✅ All {} queue workers spawned for: {}",
            worker_count, queue_name
        );

        // Wait for any worker to complete (or fail)
        while let Some(result) = worker_tasks.join_next().await {
            match result {
                Ok(Ok(())) => {
                    info!("🔄 Queue worker completed successfully");
                    // Continue waiting for other workers
                }
                Ok(Err(e)) => {
                    warn!("⚠️ Queue worker failed: {}", e);
                    // For now, continue with other workers
                    // In production, you might want different error handling
                }
                Err(e) => {
                    error!("❌ Queue worker join error: {}", e);
                }
            }
        }

        Ok(())
    }

    /// Receive messages from a queue with a single worker (internal implementation)
    async fn receive_queue_single_worker<F>(&mut self, queue_name: &str, handler: F) -> Result<()>
    where
        F: Fn(String) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
            + Send
            + Sync
            + 'static,
    {
        // Try to get queue configuration, fallback to direct path
        let queue_path = if let Some(queue_config) = self.config.get_queue_config(queue_name) {
            queue_config.path.clone()
        } else if queue_name.starts_with("/queue/") {
            queue_name.to_string()
        } else {
            format!("/queue/{}", queue_name)
        };

        info!("📥 Starting queue worker for: {}", queue_path);

        // Main reconnection loop
        loop {
            // Check if we need to connect/reconnect
            if !self.is_healthy() {
                match self.reconnect().await {
                    Ok(()) => {
                        info!(
                            "✅ Successfully connected/reconnected to queue: {}",
                            queue_path
                        );
                    }
                    Err(e) => {
                        error!(
                            "❌ Failed to connect/reconnect to queue {}: {}",
                            queue_path, e
                        );
                        return Err(e);
                    }
                }
            }

            // Ensure we have a session
            if self.session.is_none() {
                self.connect().await?;
            }

            let mut session = self
                .session
                .take()
                .ok_or_else(|| anyhow::anyhow!("No active STOMP session"))?;

            // Add this destination to active subscriptions for reconnection
            {
                let mut subs = self.active_subscriptions.lock().unwrap();
                if !subs.contains(&queue_path) {
                    subs.push(queue_path.clone());
                }
            }

            let mut connected = false;
            let mut should_reconnect = false;

            while let Some(event) = session.next_event().await {
                match event {
                    SessionEvent::Connected => {
                        info!("✅ Connected to STOMP broker for queue subscription");

                        let subscription_id = session
                            .subscription(&queue_path)
                            .with(AckMode::ClientIndividual)
                            .start()
                            .await?;

                        info!(
                            "📬 Subscribed to queue: {} (ID: {})",
                            queue_path, subscription_id
                        );
                        connected = true;
                    }

                    SessionEvent::Message {
                        destination, frame, ..
                    } => {
                        if !connected {
                            continue;
                        }

                        let body_str = std::str::from_utf8(&frame.body)
                            .unwrap_or("<non-UTF8>")
                            .to_string();

                        let message_id = frame
                            .headers
                            .headers
                            .iter()
                            .find(|h| h.0 == "message-id")
                            .map(|h| h.1.as_str())
                            .unwrap_or("unknown");

                        info!(
                            "📨 Received queue message from {}: {} (ID: {})",
                            destination,
                            body_str.chars().take(50).collect::<String>(),
                            message_id
                        );

                        // Process message with handler
                        match handler(body_str).await {
                            Ok(()) => {
                                trace!("✅ Queue message processed successfully");

                                // Acknowledge the message
                                if let Err(e) =
                                    session.acknowledge_frame(&frame, AckOrNack::Ack).await
                                {
                                    error!("❌ Failed to acknowledge queue message: {}", e);
                                } else {
                                    trace!("✓ Queue message acknowledged");
                                }
                            }
                            Err(e) => {
                                error!("❌ Queue message processing failed: {}", e);

                                // Negative acknowledge (NACK) the message
                                if let Err(nack_err) =
                                    session.acknowledge_frame(&frame, AckOrNack::Nack).await
                                {
                                    error!("❌ Failed to NACK queue message: {}", nack_err);
                                }
                            }
                        }
                    }

                    SessionEvent::Receipt { id, .. } => {
                        debug!("📄 Received receipt: {}", id);
                    }

                    SessionEvent::ErrorFrame(frame) => {
                        error!("❌ Error frame received: {:?}", frame);
                        self.mark_unhealthy();
                        should_reconnect = true;
                        break;
                    }

                    SessionEvent::Disconnected(reason) => {
                        warn!("🔌 Session disconnected: {:?}", reason);
                        self.mark_unhealthy();
                        should_reconnect = true;
                        break;
                    }

                    _ => {
                        // Ignore other events
                    }
                }
            }

            // Store the session back if we're not reconnecting
            if !should_reconnect {
                self.session = Some(session);
                break;
            } else {
                info!(
                    "🔄 Connection lost for queue {}, will attempt reconnection...",
                    queue_path
                );
                // Session will be recreated on next iteration
                sleep(Duration::from_millis(1000)).await; // Brief pause before reconnecting
            }
        }

        Ok(())
    }

    /// Disconnect from the STOMP broker
    pub async fn disconnect(&mut self) -> Result<()> {
        if let Some(mut session) = self.session.take() {
            info!("👋 Disconnecting from STOMP broker");

            if let Err(e) = session.disconnect().await {
                warn!("⚠️ Disconnect error: {}", e);
            } else {
                info!("✅ Disconnected gracefully from STOMP broker");
            }
        }

        // Mark as disconnected
        self.is_connected.store(false, Ordering::Relaxed);

        // Clear active subscriptions
        {
            let mut subs = self.active_subscriptions.lock().unwrap();
            subs.clear();
        }

        // Send shutdown signal if we have a sender
        if let Some(ref tx) = self.shutdown_tx {
            let _ = tx.send(());
        }

        Ok(())
    }

    /// Check if service is connected
    pub fn is_connected(&self) -> bool {
        self.session.is_some() && self.is_connected.load(Ordering::Relaxed)
    }

    /// Check if connection is healthy (alias for is_connected for clarity)
    pub fn is_healthy(&self) -> bool {
        self.is_connected()
    }

    /// Mark connection as unhealthy
    fn mark_unhealthy(&self) {
        self.is_connected.store(false, Ordering::Relaxed);
        warn!("🔴 Connection marked as unhealthy");
    }

    /// Determine if an error is temporary and retryable
    fn is_temporary_error(error: &anyhow::Error) -> bool {
        let error_str = error.to_string().to_lowercase();

        // Check for common temporary/network errors
        error_str.contains("connection refused")
            || error_str.contains("connection reset")
            || error_str.contains("timeout")
            || error_str.contains("network")
            || error_str.contains("broken pipe")
            || error_str.contains("connection aborted")
            || error_str.contains("host unreachable")
            || error_str.contains("no route to host")
    }

    /// Attempt to reconnect with exponential backoff
    pub async fn reconnect(&mut self) -> Result<()> {
        info!("🔄 Starting reconnection process...");

        let retry_config = self.config.retry.clone();
        let mut attempt = 0u32;

        // Mark as disconnected
        self.mark_unhealthy();
        self.session = None;

        while retry_config.should_retry(attempt) {
            // Check for shutdown signal
            if let Some(ref mut rx) = self.shutdown_rx {
                if rx.try_recv().is_ok() {
                    info!("🚨 Shutdown signal received, stopping reconnection");
                    return Err(anyhow::anyhow!("Shutdown requested during reconnection"));
                }
            }

            let delay = retry_config.calculate_delay(attempt);
            if retry_config.max_attempts < 0 {
                info!(
                    "🔁 Reconnection attempt {} (infinite retries) in {}ms",
                    attempt + 1,
                    delay.as_millis()
                );
            } else {
                info!(
                    "🔁 Reconnection attempt {} of {} in {}ms",
                    attempt + 1,
                    retry_config.max_attempts,
                    delay.as_millis()
                );
            }

            // Wait before attempting reconnection (except for first attempt)
            if attempt > 0 {
                sleep(delay).await;
            }

            // Attempt to connect
            match self.connect().await {
                Ok(()) => {
                    info!("✅ Successfully reconnected to STOMP broker");

                    // Resubscribe to previously active subscriptions
                    if let Err(e) = self.resubscribe_destinations().await {
                        warn!("⚠️ Failed to resubscribe to destinations: {}", e);
                        // Continue anyway, as basic connection is established
                    }

                    return Ok(());
                }
                Err(e) => {
                    if Self::is_temporary_error(&e) {
                        warn!(
                            "⚠️ Reconnection attempt {} failed (retryable): {}",
                            attempt + 1,
                            e
                        );
                    } else {
                        error!(
                            "❌ Reconnection attempt {} failed (non-retryable): {}",
                            attempt + 1,
                            e
                        );
                        return Err(e);
                    }
                }
            }

            attempt += 1;
        }

        let final_error = if retry_config.max_attempts < 0 {
            anyhow::anyhow!("Failed to reconnect (this should not happen with infinite retries)")
        } else {
            anyhow::anyhow!(
                "Failed to reconnect after {} attempts",
                retry_config.max_attempts
            )
        };
        Err(final_error)
    }

    /// Resubscribe to previously active destinations after reconnection
    async fn resubscribe_destinations(&mut self) -> Result<()> {
        let subscriptions = {
            let subs = self.active_subscriptions.lock().unwrap();
            subs.clone()
        };

        if subscriptions.is_empty() {
            info!("📝 No previous subscriptions to restore");
            return Ok(());
        }

        info!("🔄 Resubscribing to {} destinations", subscriptions.len());

        for destination in subscriptions {
            info!("🔄 Resubscribing to: {}", destination);
            // Note: The actual resubscription logic will be handled by the
            // individual receive_topic/receive_queue methods when they detect
            // a reconnection has occurred
        }

        Ok(())
    }

    /// Get service configuration
    pub fn get_config(&self) -> &Config {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, RetryConfig};

    #[test]
    fn test_is_temporary_error() {
        let temp_errors = vec![
            anyhow::anyhow!("Connection refused"),
            anyhow::anyhow!("Connection reset by peer"),
            anyhow::anyhow!("Network timeout occurred"),
            anyhow::anyhow!("Broken pipe error"),
            anyhow::anyhow!("Host unreachable"),
        ];

        for error in temp_errors {
            assert!(
                StompService::is_temporary_error(&error),
                "Should be temporary: {}",
                error
            );
        }

        let non_temp_errors = vec![
            anyhow::anyhow!("Invalid credentials"),
            anyhow::anyhow!("Authentication failed"),
            anyhow::anyhow!("Permission denied"),
            anyhow::anyhow!("Configuration error"),
        ];

        for error in non_temp_errors {
            assert!(
                !StompService::is_temporary_error(&error),
                "Should not be temporary: {}",
                error
            );
        }
    }

    #[test]
    fn test_retry_config_calculations() {
        let retry_config = RetryConfig {
            max_attempts: 3,
            initial_delay_ms: 100,
            max_delay_ms: 1000,
            backoff_multiplier: 2.0,
        };

        // Test delay calculations
        let delay_0 = retry_config.calculate_delay(0);
        assert_eq!(delay_0.as_millis(), 100);

        let delay_1 = retry_config.calculate_delay(1);
        assert_eq!(delay_1.as_millis(), 200); // 100 * 2^1

        let delay_2 = retry_config.calculate_delay(2);
        assert_eq!(delay_2.as_millis(), 400); // 100 * 2^2

        // Test should_retry logic with finite attempts
        assert!(retry_config.should_retry(0));
        assert!(retry_config.should_retry(1));
        assert!(retry_config.should_retry(2));
        assert!(!retry_config.should_retry(3)); // Should not retry after max_attempts

        // Test infinite retry logic
        let infinite_retry_config = RetryConfig {
            max_attempts: -1, // -1 means infinite retries
            initial_delay_ms: 100,
            max_delay_ms: 1000,
            backoff_multiplier: 2.0,
        };

        assert!(infinite_retry_config.should_retry(0));
        assert!(infinite_retry_config.should_retry(100));
        assert!(infinite_retry_config.should_retry(1000000)); // Should always retry with infinite retries
        assert!(infinite_retry_config.is_infinite_retries());
    }

    #[test]
    fn test_retry_config_delay_capping() {
        let retry_config = RetryConfig {
            max_attempts: 10,
            initial_delay_ms: 100,
            max_delay_ms: 500, // Cap at 500ms
            backoff_multiplier: 2.0,
        };

        // Test that delay is capped at max_delay_ms
        let delay_5 = retry_config.calculate_delay(5);
        // Without capping: 100 * 2^5 = 3200ms, but should be capped at 500ms
        assert_eq!(delay_5.as_millis(), 500);
    }

    #[tokio::test]
    async fn test_stomp_service_creation() {
        let config = create_test_config();
        let service = StompService::new(config).await;
        assert!(service.is_ok());

        let service = service.unwrap();
        assert!(!service.is_connected());
        assert!(!service.is_healthy());
    }

    fn create_test_config() -> Config {
        use crate::config::*;
        use std::collections::HashMap;

        Config {
            service: ServiceConfig {
                name: "test-service".to_string(),
                version: "1.0.0".to_string(),
                description: "Test service".to_string(),
            },
            broker: BrokerConfig {
                host: "localhost".to_string(),
                port: 61613,
                credentials: None,
                heartbeat: HeartbeatConfig {
                    client_send_secs: 30,
                    client_receive_secs: 30,
                },
                headers: HashMap::new(),
            },
            destinations: DestinationsConfig {
                queues: HashMap::new(),
                topics: HashMap::new(),
            },
            consumers: ConsumersConfig {
                ack_mode: "client_individual".to_string(),
                workers_per_queue: HashMap::new(),
                workers_per_topic: HashMap::new(),
            },
            logging: LoggingConfig {
                level: "info".to_string(),
                output: "stdout".to_string(),
            },
            shutdown: ShutdownConfig {
                timeout_secs: 30,
                grace_period_secs: 5,
            },
            retry: RetryConfig {
                max_attempts: 3,
                initial_delay_ms: 100,
                max_delay_ms: 1000,
                backoff_multiplier: 2.0,
            },
        }
    }
}
