mod config;
mod handler;
mod service;

use anyhow::Result;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::signal;
use tokio::sync::broadcast;
use tokio::task::JoinSet;
use tokio::time::{sleep, Duration};
use tracing::{error, info, warn};

use config::Config;
use handler::MessageHandlers;
use service::StompService;

/// Shared shutdown signal
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

/// Create a queue message handler
fn create_queue_handler(
    queue_name: &str,
) -> impl Fn(String) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
       + Clone
       + Send
       + Sync
       + 'static {
    let queue_name = queue_name.to_string();
    move |msg: String| {
        let queue_name = queue_name.clone();
        Box::pin(async move {
            let start_time = std::time::Instant::now();
            info!(
                "[{}] Processing queue message: {}",
                queue_name,
                msg.chars().take(50).collect::<String>()
            );

            let result = MessageHandlers::queue_handler(msg).await;

            let processing_time = start_time.elapsed();
            match result {
                Ok(()) => {
                    info!(
                        "[{}] ✅ Message processed successfully in {}ms",
                        queue_name,
                        processing_time.as_millis()
                    );
                }
                Err(e) => {
                    error!("[{}] ❌ Message processing failed: {}", queue_name, e);
                    return Err(e);
                }
            }
            Ok(())
        }) as Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
    }
}

/// Create a topic message handler
fn create_topic_handler(
    topic_name: &str,
) -> impl Fn(String) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
       + Clone
       + Send
       + Sync
       + 'static {
    let topic_name = topic_name.to_string();
    move |msg: String| {
        let topic_name = topic_name.clone();
        Box::pin(async move {
            let start_time = std::time::Instant::now();
            info!(
                "[{}] Processing topic message: {}",
                topic_name,
                msg.chars().take(50).collect::<String>()
            );

            let result = MessageHandlers::topic_handler(msg).await;

            let processing_time = start_time.elapsed();
            match result {
                Ok(()) => {
                    info!(
                        "[{}] ✅ Message processed successfully in {}ms",
                        topic_name,
                        processing_time.as_millis()
                    );
                }
                Err(e) => {
                    error!("[{}] ❌ Message processing failed: {}", topic_name, e);
                    return Err(e);
                }
            }
            Ok(())
        }) as Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize structured logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_target(false)
        .with_thread_ids(false)
        .with_line_number(false)
        .init();

    // Load configuration
    let config = Config::load("config.yaml")?;

    info!(
        "🚀 Starting Multi-Subscriber STOMP Application: {}",
        config.service.name
    );
    info!("📋 Version: {}", config.service.version);
    info!("📄 Description: {}", config.service.description);
    info!("🔗 Broker: {}:{}", config.broker.host, config.broker.port);

    // Create shutdown broadcast channel
    let (shutdown_tx, _shutdown_rx) = broadcast::channel::<()>(1);

    // Create task collection for managing subscribers
    let mut subscriber_tasks = JoinSet::new();

    // Get all configured queues and topics
    let queue_names = config.get_all_queue_names();
    let topic_names = config.get_all_topic_names();

    info!(
        "🔧 Setting up {} queues and {} topics...",
        queue_names.len(),
        topic_names.len()
    );

    // Start subscribers for each queue (workers will be managed internally by service)
    for queue_name in queue_names {
        let worker_count = config.get_queue_workers(&queue_name);
        info!(
            "📊 Queue '{}' configured with {} workers",
            queue_name, worker_count
        );

        let config_clone = config.clone();
        let _shutdown_rx = shutdown_tx.subscribe();
        let queue_name_clone = queue_name.clone();

        subscriber_tasks.spawn(async move {
            let mut service = StompService::new(config_clone).await?;
            service
                .receive_queue(&queue_name_clone, create_queue_handler(&queue_name_clone))
                .await
        });
    }

    // Start subscribers for each topic (workers will be managed internally by service)
    for topic_name in topic_names {
        let worker_count = config.get_topic_workers(&topic_name);
        info!(
            "📊 Topic '{}' configured with {} workers",
            topic_name, worker_count
        );

        let config_clone = config.clone();
        let _shutdown_rx = shutdown_tx.subscribe();
        let topic_name_clone = topic_name.clone();

        subscriber_tasks.spawn(async move {
            let mut service = StompService::new(config_clone).await?;
            service
                .receive_topic(&topic_name_clone, create_topic_handler(&topic_name_clone))
                .await
        });
    }

    // Wait a bit before starting message sending
    sleep(Duration::from_secs(2)).await;

    // Create STOMP service for sending messages
    let mut stomp_service = StompService::new(config.clone()).await?;

    // Setup graceful shutdown signal handler
    let shutdown_handle = {
        let shutdown_tx = shutdown_tx.clone();
        tokio::spawn(async move {
            setup_signal_handlers().await;
            let _ = shutdown_tx.send(()); // Notify all subscribers to shutdown
        })
    };

    // Demonstrate sending messages
    info!("📤 Sending test messages to multiple subscribers...");

    // Send to topic
    let mut topic_headers = HashMap::new();
    topic_headers.insert("content-type".to_string(), "text/plain".to_string());
    topic_headers.insert("priority".to_string(), "high".to_string());

    if let Err(e) = stomp_service
        .send_topic(
            "notifications",
            "Test topic message for multi-subscribers",
            topic_headers,
        )
        .await
    {
        error!("Failed to send topic message: {}", e);
    }

    // Send to queue
    let mut queue_headers = HashMap::new();
    queue_headers.insert("content-type".to_string(), "text/plain".to_string());
    queue_headers.insert("persistent".to_string(), "true".to_string());

    if let Err(e) = stomp_service
        .send_queue(
            "default",
            "Test queue message for multi-subscribers",
            queue_headers,
        )
        .await
    {
        error!("Failed to send queue message: {}", e);
    }

    info!("✅ Test messages sent successfully");
    info!("🔄 Multi-subscriber system running... Press Ctrl+C to shutdown gracefully");

    // Wait for shutdown signal
    let _ = shutdown_handle.await;

    info!("🛑 Shutdown signal received, stopping all subscribers...");

    // Give tasks time to finish gracefully
    let shutdown_timeout = Duration::from_secs(config.shutdown.timeout_secs as u64);
    let mut shutdown_count = 0;
    let total_subscribers = subscriber_tasks.len();

    // Wait for all subscriber tasks with timeout
    let shutdown_start = tokio::time::Instant::now();
    while !subscriber_tasks.is_empty() && shutdown_start.elapsed() < shutdown_timeout {
        match tokio::time::timeout(Duration::from_secs(1), subscriber_tasks.join_next()).await {
            Ok(Some(result)) => {
                shutdown_count += 1;
                match result {
                    Ok(Ok(())) => {
                        info!(
                            "✅ Subscriber {}/{} shut down gracefully",
                            shutdown_count, total_subscribers
                        );
                    }
                    Ok(Err(e)) => {
                        warn!(
                            "⚠️ Subscriber {}/{} shut down with error: {}",
                            shutdown_count, total_subscribers, e
                        );
                    }
                    Err(e) => {
                        warn!(
                            "⚠️ Subscriber {}/{} join error: {}",
                            shutdown_count, total_subscribers, e
                        );
                    }
                }
            }
            Ok(None) => break, // No more tasks
            Err(_) => {
                // Timeout waiting for next task - continue checking
                continue;
            }
        }
    }

    // Abort any remaining tasks
    if !subscriber_tasks.is_empty() {
        warn!(
            "⏰ Shutdown timeout reached, force stopping {} remaining subscribers",
            subscriber_tasks.len()
        );
        subscriber_tasks.abort_all();
        while let Some(result) = subscriber_tasks.join_next().await {
            if let Err(e) = result {
                if !e.is_cancelled() {
                    warn!("Force-stopped subscriber error: {}", e);
                }
            }
        }
    }

    // Clean disconnect of main service
    if let Err(e) = stomp_service.disconnect().await {
        warn!("Main service disconnect error: {}", e);
    }

    info!(
        "✅ Multi-subscriber application shutdown complete ({} subscribers stopped)",
        total_subscribers
    );
    Ok(())
}

/// Setup signal handlers for graceful shutdown
async fn setup_signal_handlers() {
    #[cfg(unix)]
    {
        use signal::unix::{signal, SignalKind};
        let mut sigterm =
            signal(SignalKind::terminate()).expect("Failed to register SIGTERM handler");
        let mut sigint =
            signal(SignalKind::interrupt()).expect("Failed to register SIGINT handler");

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
