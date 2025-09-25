use anyhow::{Context, Result};
use std::collections::HashMap;
use std::pin::Pin;
use tracing::{info, warn, debug, error, trace};
use uuid::Uuid;
use chrono::Utc;

use stomp::session_builder::SessionBuilder;
use stomp::session::{SessionEvent, Session};
use stomp::subscription::{AckOrNack, AckMode};
use stomp::connection::{HeartBeat, Credentials};
use stomp::header::Header;
use stomp::option_setter::OptionSetter;

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
}

impl StompService {
    /// Create a new STOMP service instance
    pub async fn new(config: Config) -> Result<Self> {
        info!("🚀 Initializing STOMP service: {}", config.service.name);
        
        Ok(Self {
            config,
            session: None,
        })
    }
    
    /// Connect to the STOMP broker using the simplified pattern from reference
    pub async fn connect(&mut self) -> Result<()> {
        let (send_heartbeat, recv_heartbeat) = self.config.get_heartbeat_ms();
        info!("🔗 Connecting to STOMP broker at {}:{}", 
              self.config.broker.host, self.config.broker.port);
        
        // Build session using the reference pattern with option setters
        let mut session_builder = SessionBuilder::new(&self.config.broker.host, self.config.broker.port)
            .with(WithHeartBeat(HeartBeat(send_heartbeat, recv_heartbeat)));
        
        // Add credentials if available
        if let Some((username, password)) = self.config.get_credentials() {
            session_builder = session_builder.with(WithCredentials(Credentials(&username, &password)));
            info!("🔐 Using credentials: {}", username);
        }
        
        // Add client ID header  
        let client_id = format!("{}-{}", self.config.service.name, Uuid::new_v4());
        session_builder = session_builder.with(WithHeader(Header::new("custom-client-id", &client_id)));
        
        // Add custom headers
        for (key, value) in &self.config.broker.headers {
            session_builder = session_builder.with(WithHeader(Header::new(key, value)));
        }
        
        let session = session_builder
            .start()
            .await
            .with_context(|| "Failed to establish STOMP connection")?;
        
        self.session = Some(session);
        info!("✅ Successfully connected to STOMP broker with client-id: {}", client_id);
        
        Ok(())
    }
    
    /// Send message to a topic (simplified pattern following reference)
    pub async fn send_topic(&mut self, topic_name: &str, payload: &str, headers: HashMap<String, String>) -> Result<()> {
        // Ensure we have an active session
        if self.session.is_none() {
            self.connect().await?;
        }
        
        let session = self.session.as_mut()
            .ok_or_else(|| anyhow::anyhow!("No active STOMP session"))?;
        
        // Build destination path - try config first, then fallback to direct path
        let destination = if let Some(topic_config) = self.config.get_topic_config(topic_name) {
            topic_config.path.clone()
        } else {
            if topic_name.starts_with("/topic/") {
                topic_name.to_string()
            } else {
                format!("/topic/{}", topic_name)
            }
        };
        
        info!("📤 Sending message to topic: {}", destination);
        
        // Send message using new comprehensive header support API with fluent chaining
        let timestamp = Utc::now().to_rfc3339();
        
        let mut message_builder = session.message(&destination, payload);
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
    pub async fn send_queue(&mut self, queue_name: &str, payload: &str, headers: HashMap<String, String>) -> Result<()> {
        // Ensure we have an active session
        if self.session.is_none() {
            self.connect().await?;
        }
        
        let session = self.session.as_mut()
            .ok_or_else(|| anyhow::anyhow!("No active STOMP session"))?;
        
        // Build destination path - try config first, then fallback to direct path
        let destination = if let Some(queue_config) = self.config.get_queue_config(queue_name) {
            queue_config.path.clone()
        } else {
            if queue_name.starts_with("/queue/") {
                queue_name.to_string()
            } else {
                format!("/queue/{}", queue_name)
            }
        };
        
        info!("📤 Sending message to queue: {}", destination);
        
        // Send message using new comprehensive header support API with advanced features
        let timestamp = Utc::now().to_rfc3339();
        
        let mut message_builder = session.message(&destination, payload);
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
    
    /// Receive messages from a topic with a handler function
    pub async fn receive_topic<F>(&mut self, topic_name: &str, handler: F) -> Result<()>
    where
        F: Fn(String) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> + Send + Sync + 'static,
    {
        if self.session.is_none() {
            self.connect().await?;
        }
        
        let mut session = self.session.take()
            .ok_or_else(|| anyhow::anyhow!("No active STOMP session"))?;
        
        // Try to get topic configuration, fallback to direct path
        let topic_path = if let Some(topic_config) = self.config.get_topic_config(topic_name) {
            topic_config.path.clone()
        } else {
            if topic_name.starts_with("/topic/") {
                topic_name.to_string()
            } else {
                format!("/topic/{}", topic_name)
            }
        };
        
        info!("📥 Setting up topic subscription: {}", topic_path);
        
        let mut connected = false;
        
        while let Some(event) = session.next_event().await {
            match event {
                SessionEvent::Connected => {
                    info!("✅ Connected to STOMP broker for topic subscription");
                    
                    let subscription_id = session
                        .subscription(&topic_path)
                        .with(AckMode::ClientIndividual)
                        .start()
                        .await?;
                    
                    info!("📬 Subscribed to topic: {} (ID: {})", topic_path, subscription_id);
                    connected = true;
                }
                
                SessionEvent::Message { destination, frame, .. } => {
                    if !connected {
                        continue;
                    }
                    
                    let body_str = std::str::from_utf8(&frame.body)
                        .unwrap_or("<non-UTF8>")
                        .to_string();
                    
                    let message_id = frame.headers.headers.iter()
                        .find(|h| h.0 == "message-id")
                        .map(|h| h.1.as_str())
                        .unwrap_or("unknown");
                    
                    info!("📨 Received topic message from {}: {} (ID: {})", 
                          destination, 
                          body_str.chars().take(50).collect::<String>(),
                          message_id);
                    
                    // Process message with handler
                    match handler(body_str).await {
                        Ok(()) => {
                            trace!("✅ Topic message processed successfully");
                            
                            // Acknowledge the message
                            if let Err(e) = session.acknowledge_frame(&frame, AckOrNack::Ack).await {
                                error!("❌ Failed to acknowledge topic message: {}", e);
                            } else {
                                trace!("✓ Topic message acknowledged");
                            }
                        }
                        Err(e) => {
                            error!("❌ Topic message processing failed: {}", e);
                            
                            // Negative acknowledge (NACK) the message
                            if let Err(nack_err) = session.acknowledge_frame(&frame, AckOrNack::Nack).await {
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
                    break;
                }
                
                SessionEvent::Disconnected(reason) => {
                    warn!("🔌 Session disconnected: {:?}", reason);
                    break;
                }
                
                _ => {
                    // Ignore other events
                }
            }
        }
        
        Ok(())
    }
    
    /// Receive messages from a queue with a handler function
    pub async fn receive_queue<F>(&mut self, queue_name: &str, handler: F) -> Result<()>
    where
        F: Fn(String) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> + Send + Sync + 'static,
    {
        if self.session.is_none() {
            self.connect().await?;
        }
        
        let mut session = self.session.take()
            .ok_or_else(|| anyhow::anyhow!("No active STOMP session"))?;
        
        // Try to get queue configuration, fallback to direct path
        let queue_path = if let Some(queue_config) = self.config.get_queue_config(queue_name) {
            queue_config.path.clone()
        } else {
            if queue_name.starts_with("/queue/") {
                queue_name.to_string()
            } else {
                format!("/queue/{}", queue_name)
            }
        };
        
        info!("📥 Setting up queue subscription: {}", queue_path);
        
        let mut connected = false;
        
        while let Some(event) = session.next_event().await {
            match event {
                SessionEvent::Connected => {
                    info!("✅ Connected to STOMP broker for queue subscription");
                    
                    let subscription_id = session
                        .subscription(&queue_path)
                        .with(AckMode::ClientIndividual)
                        .start()
                        .await?;
                    
                    info!("📬 Subscribed to queue: {} (ID: {})", queue_path, subscription_id);
                    connected = true;
                }
                
                SessionEvent::Message { destination, frame, .. } => {
                    if !connected {
                        continue;
                    }
                    
                    let body_str = std::str::from_utf8(&frame.body)
                        .unwrap_or("<non-UTF8>")
                        .to_string();
                    
                    let message_id = frame.headers.headers.iter()
                        .find(|h| h.0 == "message-id")
                        .map(|h| h.1.as_str())
                        .unwrap_or("unknown");
                    
                    info!("📨 Received queue message from {}: {} (ID: {})", 
                          destination, 
                          body_str.chars().take(50).collect::<String>(),
                          message_id);
                    
                    // Process message with handler
                    match handler(body_str).await {
                        Ok(()) => {
                            trace!("✅ Queue message processed successfully");
                            
                            // Acknowledge the message
                            if let Err(e) = session.acknowledge_frame(&frame, AckOrNack::Ack).await {
                                error!("❌ Failed to acknowledge queue message: {}", e);
                            } else {
                                trace!("✓ Queue message acknowledged");
                            }
                        }
                        Err(e) => {
                            error!("❌ Queue message processing failed: {}", e);
                            
                            // Negative acknowledge (NACK) the message
                            if let Err(nack_err) = session.acknowledge_frame(&frame, AckOrNack::Nack).await {
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
                    break;
                }
                
                SessionEvent::Disconnected(reason) => {
                    warn!("🔌 Session disconnected: {:?}", reason);
                    break;
                }
                
                _ => {
                    // Ignore other events
                }
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
        
        Ok(())
    }
    
    /// Check if service is connected
    pub fn is_connected(&self) -> bool {
        self.session.is_some()
    }
    
    /// Get service configuration
    pub fn get_config(&self) -> &Config {
        &self.config
    }
}