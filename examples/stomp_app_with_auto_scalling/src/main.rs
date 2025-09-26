use anyhow::Result;
use stomp_app_with_auto_scalling::runner::StompRunner;
use stomp_app_with_auto_scalling::utils;
use tracing::info;

// Custom handler for processing order messages
async fn handle_order_message(message: String) -> Result<()> {
    info!("🛒 Processing ORDER: {}", message);
    // Simulate some processing time
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    Ok(())
}

// Custom handler for processing notification messages
async fn handle_notification_message(message: String) -> Result<()> {
    info!("🔔 Processing NOTIFICATION: {}", message);
    // Simulate some processing time
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    Ok(())
}

// Custom handler for processing API request messages
async fn handle_api_request_message(message: String) -> Result<()> {
    info!("🌐 Processing API REQUEST: {}", message);
    // Simulate API processing
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging first
    utils::initialize_logging();
    
    // Load configuration
    let config = utils::load_configuration("config.yaml")?;
    
    // Display startup information
    utils::display_startup_info(&config);
    
    // Clone config for sending test messages
    let config_for_test = config.clone();
    
    // Start sending test messages in background
    tokio::spawn(async move {
        utils::send_test_messages(&config_for_test).await;
    });
    
    // Example 1: Use configuration with custom handlers
    StompRunner::new()
        .with_config(config)
        .add_queue("orders", handle_order_message)  // Custom handler for orders queue
        .add_topic("notifications", handle_notification_message)  // Custom handler for notifications topic
        .add_auto_scaling_queue("api_requests", handle_api_request_message)  // Custom handler for auto-scaling queue
        .run()
        .await

    // Alternative examples (commented out):
    
    // Example 2: Simple setup with all defaults
    /*
    StompRunner::new()
        .with_config_file("config.yaml")?
        .run()
        .await
    */
    
    // Example 3: Custom configuration with multiple handlers
    /*
    // Send test messages in background if needed
    let config_for_test = utils::load_configuration("config.yaml")?;
    tokio::spawn(async move {
        utils::send_test_messages(&config_for_test).await;
    });
    
    StompRunner::new()
        .with_config_file("config.yaml")?
        .add_queue("user_events", |msg| async move {
            info!("👤 User event: {}", msg);
            Ok(())
        })
        .add_queue("system_logs", |msg| async move {
            info!("📋 System log: {}", msg);
            Ok(())
        })
        .add_auto_scaling_queue("high_load_queue", |msg| async move {
            info!("⚡ High load processing: {}", msg);
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            Ok(())
        })
        .run()
        .await
    */
}

