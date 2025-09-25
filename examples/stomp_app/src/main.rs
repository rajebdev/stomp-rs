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

/// Subscriber type enumeration
#[derive(Debug, Clone, PartialEq)]
enum SubscriberType {
    Queue,
    Topic,
}

/// Run a single subscriber instance with auto-reconnection
async fn run_subscriber(
    dest_name: String,
    subscriber_id: String,
    subscriber_type: SubscriberType,
    config: Config,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> Result<()> {
    let log_prefix = format!("[{}][{}]", dest_name, subscriber_id);

    info!("{} Starting subscriber with auto-reconnection...", log_prefix);

    // Create dedicated STOMP service for this subscriber
    let mut service = match StompService::new(config.clone()).await {
        Ok(service) => service,
        Err(e) => {
            error!("{} Failed to create STOMP service: {}", log_prefix, e);
            return Err(e);
        }
    };

    // Main subscriber loop with reconnection handling
    loop {
        // Check for shutdown signal first
        if let Ok(_) = shutdown_rx.try_recv() {
            info!("{} Received shutdown signal, stopping subscriber...", log_prefix);
            break;
        }

        // Ensure we're connected before attempting to subscribe
        if !service.is_healthy() {
            info!("{} Connection unhealthy, attempting reconnection...", log_prefix);
            match service.reconnect().await {
                Ok(()) => {
                    info!("{} Reconnection successful, resuming subscription", log_prefix);
                }
                Err(e) => {
                    if is_shutdown_requested() {
                        info!("{} Shutdown requested during reconnection, exiting", log_prefix);
                        break;
                    }
                    error!("{} Reconnection failed permanently: {}", log_prefix, e);
                    return Err(e);
                }
            }
        }

        // Define the message handler with proper logging prefix
        let handler = {
            let log_prefix = log_prefix.clone();
            let subscriber_type = subscriber_type.clone();

            move |msg: String| {
                let log_prefix = log_prefix.clone();
                let subscriber_type = subscriber_type.clone();

                Box::pin(async move {
                    let start_time = std::time::Instant::now();

                    info!(
                        "{} Processing {} message: {}",
                        log_prefix,
                        match subscriber_type {
                            SubscriberType::Queue => "queue",
                            SubscriberType::Topic => "topic",
                        },
                        msg.chars().take(50).collect::<String>()
                    );

                    // Call appropriate handler based on type
                    let result = match subscriber_type {
                        SubscriberType::Queue => MessageHandlers::queue_handler(msg).await,
                        SubscriberType::Topic => MessageHandlers::topic_handler(msg).await,
                    };

                    let processing_time = start_time.elapsed();
                    match result {
                        Ok(()) => {
                            info!(
                                "{} ✅ Message processed successfully in {}ms",
                                log_prefix,
                                processing_time.as_millis()
                            );
                        }
                        Err(e) => {
                            error!("{} ❌ Message processing failed: {}", log_prefix, e);
                            return Err(e);
                        }
                    }

                    Ok(())
                }) as Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
            }
        };

        // Attempt to start the appropriate subscription with error handling
        let receive_future = async {
            match subscriber_type {
                SubscriberType::Queue => {
                    info!("{} 📥 Starting queue subscription...", log_prefix);
                    service.receive_queue(&dest_name, handler).await
                }
                SubscriberType::Topic => {
                    info!("{} 📥 Starting topic subscription...", log_prefix);
                    service.receive_topic(&dest_name, handler).await
                }
            }
        };

        // Wait for either the receive future to complete or shutdown signal
        let subscription_result = tokio::select! {
            result = receive_future => result,
            _ = shutdown_rx.recv() => {
                info!("{} Received shutdown signal during subscription", log_prefix);
                break;
            }
        };

        // Handle subscription results
        match subscription_result {
            Ok(_) => {
                info!("{} Subscription completed normally, checking connection health...", log_prefix);
                // If subscription ended normally, check if it was due to connection issues
                if !service.is_healthy() {
                    warn!("{} Subscription ended due to connection loss, will attempt reconnection", log_prefix);
                    // Continue the loop to attempt reconnection
                    sleep(Duration::from_millis(1000)).await; // Brief pause before next iteration
                    continue;
                }
                // If healthy disconnection, break the loop
                break;
            }
            Err(e) => {
                if is_shutdown_requested() {
                    info!("{} Subscription stopped due to shutdown request", log_prefix);
                    break;
                }
                
                // Check if error is retryable
                let error_str = e.to_string().to_lowercase();
                let is_retryable = error_str.contains("connection") || 
                                  error_str.contains("network") || 
                                  error_str.contains("timeout") ||
                                  error_str.contains("broken pipe") ||
                                  error_str.contains("reset");
                
                if is_retryable {
                    warn!("{} Subscription failed with retryable error: {}, marking connection unhealthy", log_prefix, e);
                    // Connection will be handled at the start of the next loop iteration
                    sleep(Duration::from_millis(1000)).await; // Brief pause before retry
                    continue;
                } else {
                    error!("{} Subscription failed with non-retryable error: {}", log_prefix, e);
                    return Err(e);
                }
            }
        }
    }

    // Clean disconnect
    if let Err(e) = service.disconnect().await {
        warn!("{} Disconnect error: {}", log_prefix, e);
    } else {
        info!("{} Disconnected successfully", log_prefix);
    }

    Ok(())
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

    // Create task collection for managing multiple subscribers
    let mut subscriber_tasks = JoinSet::new();

    // Build list of all subscribers to spawn
    let mut subscribers_to_spawn = Vec::new();

    // Add queue subscribers
    for (queue_name, worker_count) in config.get_all_queue_workers() {
        info!(
            "📊 Queue '{}' configured with {} workers",
            queue_name, worker_count
        );
        for worker_id in 0..worker_count {
            let subscriber_id = format!("{}#{}", queue_name, worker_id + 1);
            subscribers_to_spawn.push((queue_name.clone(), subscriber_id, SubscriberType::Queue));
        }
    }

    // Add topic subscribers
    for (topic_name, worker_count) in config.get_all_topic_workers() {
        info!(
            "📊 Topic '{}' configured with {} workers",
            topic_name, worker_count
        );
        for worker_id in 0..worker_count {
            let subscriber_id = format!("{}#{}", topic_name, worker_id + 1);
            subscribers_to_spawn.push((topic_name.clone(), subscriber_id, SubscriberType::Topic));
        }
    }

    info!(
        "🔧 Spawning {} total subscribers...",
        subscribers_to_spawn.len()
    );

    // Spawn all subscriber tasks
    for (dest_name, subscriber_id, sub_type) in subscribers_to_spawn {
        let config_clone = config.clone();
        let shutdown_rx = shutdown_tx.subscribe();

        subscriber_tasks.spawn(run_subscriber(
            dest_name,
            subscriber_id,
            sub_type,
            config_clone,
            shutdown_rx,
        ));
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

/// Check if shutdown has been requested
fn is_shutdown_requested() -> bool {
    SHUTDOWN_REQUESTED.load(Ordering::Relaxed)
}
