mod config;
mod service;
mod handler;

use anyhow::Result;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::{info, error, warn};
use tokio::signal;
use tokio::time::{sleep, Duration};

use config::Config;
use service::StompService;
use handler::MessageHandlers;

/// Shared shutdown signal
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize structured logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into())
        )
        .with_target(false)
        .with_thread_ids(false)
        .with_line_number(false)
        .init();
    
    // Load configuration
    let config = Config::load("config.yaml")?;
    
    info!("🚀 Starting STOMP Application: {}", config.service.name);
    info!("📋 Version: {}", config.service.version);
    info!("📄 Description: {}", config.service.description);
    info!("🔗 Broker: {}:{}", config.broker.host, config.broker.port);
    
    // Wait a bit before starting consumers
    sleep(Duration::from_secs(1)).await;
    
    // Create separate service instances for receiving (since receive methods take ownership)
    let config_clone = config.clone();
    let topic_service_handle = tokio::spawn(async move {
        let mut topic_service = match StompService::new(config_clone).await {
            Ok(service) => service,
            Err(e) => {
                error!("Failed to create topic service: {}", e);
                return;
            }
        };
        
        info!("📥 Starting topic consumer...");
        if let Err(e) = topic_service.receive_topic(
            "notifications",
            |msg| Box::pin(MessageHandlers::topic_handler(msg))
        ).await {
            if !is_shutdown_requested() {
                error!("Topic consumer error: {}", e);
            } else {
                info!("Topic consumer stopped gracefully");
            }
        }
    });
    
    let config_clone2 = config.clone();
    let queue_service_handle = tokio::spawn(async move {
        let mut queue_service = match StompService::new(config_clone2).await {
            Ok(service) => service,
            Err(e) => {
                error!("Failed to create queue service: {}", e);
                return;
            }
        };
        
        info!("📥 Starting queue consumer...");
        if let Err(e) = queue_service.receive_queue(
            "default",
            |msg| Box::pin(MessageHandlers::queue_handler(msg))
        ).await {
            if !is_shutdown_requested() {
                error!("Queue consumer error: {}", e);
            } else {
                info!("Queue consumer stopped gracefully");
            }
        }
    });
    
    info!("🔄 Application running... Press Ctrl+C to shutdown gracefully");

    
    
    // Create STOMP service
    let mut stomp_service = StompService::new(config.clone()).await?;
    
    // Initialize message handlers
    let _handlers = MessageHandlers::new();
    
    // Setup graceful shutdown signal handler
    let shutdown_handle = tokio::spawn(async {
        setup_signal_handlers().await;
    });
    
    // Demonstrate sending messages
    info!("📤 Sending test messages...");
    
    // Send to topic
    let mut topic_headers = HashMap::new();
    topic_headers.insert("content-type".to_string(), "text/plain".to_string());
    topic_headers.insert("priority".to_string(), "high".to_string());
    
    if let Err(e) = stomp_service.send_topic(
        "notifications", 
        "Animal", 
        topic_headers
    ).await {
        error!("Failed to send topic message: {}", e);
    }
    
    // Send to queue
    let mut queue_headers = HashMap::new();
    queue_headers.insert("content-type".to_string(), "text/plain".to_string());
    queue_headers.insert("persistent".to_string(), "true".to_string());
    
    if let Err(e) = stomp_service.send_queue(
        "default", 
        "Animal", 
        queue_headers
    ).await {
        error!("Failed to send queue message: {}", e);
    }
    
    info!("✅ Test messages sent successfully");
    
    // Wait for shutdown signal
    let _ = shutdown_handle.await;
    
    info!("🛑 Shutdown signal received, stopping services...");
    
    // Give tasks time to finish gracefully
    let shutdown_timeout = Duration::from_secs(config.shutdown.timeout_secs as u64);
    
    // Wait for all tasks with timeout
    match tokio::time::timeout(
        shutdown_timeout,
        futures::future::join_all(vec![topic_service_handle, queue_service_handle])
    ).await {
        Ok(results) => {
            for (i, result) in results.into_iter().enumerate() {
                if let Err(e) = result {
                    if !e.is_cancelled() {
                        error!("Task {} error: {}", i, e);
                    }
                }
            }
        }
        Err(_) => {
            warn!("⏰ Shutdown timeout reached, force stopping remaining tasks");
        }
    }
    
    // Clean disconnect of main service
    if let Err(e) = stomp_service.disconnect().await {
        warn!("Main service disconnect error: {}", e);
    }
    
    info!("✅ Application shutdown complete");
    Ok(())
}

/// Setup signal handlers for graceful shutdown
async fn setup_signal_handlers() {
    #[cfg(unix)]
    {
        use signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate()).expect("Failed to register SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to register SIGINT handler");
        
        tokio::select! {
            _ = sigterm.recv() => {
                info!("📡 Received SIGTERM - initiating graceful shutdown");
                request_shutdown();
            }
            _ = sigint.recv() => {
                info!("📡 Received SIGINT (Ctrl+C) - initiating graceful shutdown");
                request_shutdown();
            }
        }
    }
    
    #[cfg(windows)]
    {
        match signal::ctrl_c().await {
            Ok(()) => {
                info!("📡 Received Ctrl+C - initiating graceful shutdown");
                request_shutdown();
            }
            Err(err) => {
                error!("Unable to listen for shutdown signal: {}", err);
            }
        }
    }
}

/// Request application shutdown
fn request_shutdown() {
    SHUTDOWN_REQUESTED.store(true, Ordering::Relaxed);
}

/// Check if shutdown has been requested
fn is_shutdown_requested() -> bool {
    SHUTDOWN_REQUESTED.load(Ordering::Relaxed)
}
