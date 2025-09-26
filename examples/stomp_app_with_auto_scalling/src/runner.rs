use anyhow::Result;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::future::Future;
use tokio::sync::broadcast;
use tokio::task::JoinSet;
use tokio::time::Duration;
use tracing::{error, info, warn};

use crate::config::Config;
use crate::service::StompService;
use crate::consumer_pool::{ConsumerPool, MessageHandler};
use crate::autoscaler::AutoScaler;


/// Type alias for message handler function
pub type MessageHandlerFn = Arc<dyn Fn(String) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync + 'static>;

/// Type alias for auto-scaling message handler function  
pub type AutoScaleHandlerFn = Arc<dyn Fn(String) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync + 'static>;

/// Configuration for a queue with custom handler
pub struct QueueConfig {
    pub name: String,
    pub handler: Option<MessageHandlerFn>,
    pub auto_scaling: bool,
}

/// Configuration for a topic with custom handler
pub struct TopicConfig {
    pub name: String,
    pub handler: Option<MessageHandlerFn>,
}

/// Builder for configuring and running the STOMP application
pub struct StompRunner {
    config: Option<Config>,
    queue_configs: Vec<QueueConfig>,
    topic_configs: Vec<TopicConfig>,
    auto_scale_handlers: HashMap<String, AutoScaleHandlerFn>,
}

impl Default for StompRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl StompRunner {
    /// Create a new STOMP runner instance
    pub fn new() -> Self {
        Self {
            config: None,
            queue_configs: Vec::new(),
            topic_configs: Vec::new(),
            auto_scale_handlers: HashMap::new(),
        }
    }

    /// Load configuration from file
    pub fn with_config_file(mut self, config_path: &str) -> Result<Self> {
        self.config = Some(Config::load(config_path)?);
        Ok(self)
    }

    /// Set configuration directly
    pub fn with_config(mut self, config: Config) -> Self {
        self.config = Some(config);
        self
    }


    /// Add a queue with a custom handler for static mode
    pub fn add_queue<F, Fut>(mut self, queue_name: &str, handler: F) -> Self 
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let handler_fn: MessageHandlerFn = Arc::new(move |msg| {
            Box::pin(handler(msg))
        });
        
        self.queue_configs.push(QueueConfig {
            name: queue_name.to_string(),
            handler: Some(handler_fn),
            auto_scaling: false,
        });
        self
    }


    /// Add an auto-scaling queue with custom handler
    pub fn add_auto_scaling_queue<F, Fut>(mut self, queue_name: &str, handler: F) -> Self 
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let handler_fn: AutoScaleHandlerFn = Arc::new(move |msg| {
            Box::pin(handler(msg))
        });
        
        self.auto_scale_handlers.insert(queue_name.to_string(), handler_fn);
        
        self.queue_configs.push(QueueConfig {
            name: queue_name.to_string(),
            handler: None, // Auto-scaling uses the handler in auto_scale_handlers
            auto_scaling: true,
        });
        self
    }


    /// Add a topic with custom handler
    pub fn add_topic<F, Fut>(mut self, topic_name: &str, handler: F) -> Self 
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let handler_fn: MessageHandlerFn = Arc::new(move |msg| {
            Box::pin(handler(msg))
        });
        
        self.topic_configs.push(TopicConfig {
            name: topic_name.to_string(),
            handler: Some(handler_fn),
        });
        self
    }


    /// Run the STOMP application with the configured settings
    pub async fn run(mut self) -> Result<()> {
        // Get configuration
        let config = self.config.take().unwrap_or_else(|| {
            Config::load("config.yaml").expect("Failed to load default config")
        });

        // Determine if any queues are configured for auto-scaling
        let has_auto_scaling = self.queue_configs.iter().any(|q| q.auto_scaling) || 
                               config.is_auto_scaling_enabled();

        if has_auto_scaling {
            info!("🎯 Auto-scaling is ENABLED");
            self.run_with_auto_scaling(config).await
        } else {
            info!("📊 Auto-scaling is DISABLED, using static worker configuration");
            self.run_static_workers(config).await
        }
    }

    /// Run with auto-scaling enabled
    async fn run_with_auto_scaling(self, config: Config) -> Result<()> {
        info!("🎯 Starting application with auto-scaling mode");

        // Create shutdown broadcast channel
        let (shutdown_tx, _shutdown_rx) = broadcast::channel::<()>(1);

        // Setup auto-scaling consumer pools
        let consumer_pools = self.setup_custom_consumer_pools(&config, &shutdown_tx).await?;

        if consumer_pools.is_empty() {
            warn!("No queues configured for auto-scaling");
            return Ok(());
        }

        info!("✅ All consumer pools initialized");

        // Handle topics with static workers (if any)
        let topic_tasks = self.setup_custom_topic_subscribers(&config, &shutdown_tx).await;

        // Create and start auto-scaler
        let autoscaler = AutoScaler::new(
            config.clone(),
            consumer_pools,
            shutdown_tx.subscribe(),
        )?;

        // Setup graceful shutdown signal handler
        let shutdown_handle = self.setup_shutdown_handler(shutdown_tx.clone());

        // Start auto-scaler in background
        let autoscaler_handle = self.start_autoscaler(autoscaler).await;


        info!("🔄 Auto-scaling system running... Press Ctrl+C to shutdown gracefully");

        // Wait for shutdown and cleanup
        self.shutdown_autoscaling_system(shutdown_handle, autoscaler_handle, topic_tasks).await
    }

    /// Run with static workers
    async fn run_static_workers(self, config: Config) -> Result<()> {
        info!("📊 Starting application with static worker mode");
        
        // Create shutdown broadcast channel
        let (shutdown_tx, _shutdown_rx) = broadcast::channel::<()>(1);

        // Create task collection for managing subscribers
        let mut subscriber_tasks = JoinSet::new();

        // Setup static subscribers for queues and topics
        self.setup_custom_static_subscribers(&config, &shutdown_tx, &mut subscriber_tasks).await;

        // Setup graceful shutdown signal handler
        let shutdown_handle = self.setup_shutdown_handler(shutdown_tx.clone());


        info!("🔄 Static worker system running... Press Ctrl+C to shutdown gracefully");

        // Wait for shutdown signal
        let _ = shutdown_handle.await;

        info!("🛑 Shutdown signal received, stopping all subscribers...");

        // Shutdown static workers gracefully
        self.shutdown_static_workers(subscriber_tasks, &config).await;

        info!("✅ Static worker application shutdown complete");
        Ok(())
    }

    /// Setup consumer pools with custom handlers for auto-scaling queues
    async fn setup_custom_consumer_pools(
        &self,
        config: &Config,
        shutdown_tx: &broadcast::Sender<()>,
    ) -> Result<HashMap<String, ConsumerPool>> {
        let mut consumer_pools = HashMap::new();
        
        // Get auto-scaling queues from config
        let config_auto_queues = config.get_auto_scaling_queues();
        
        // Combine custom queues and config queues
        let mut all_auto_queues = Vec::new();
        
        // Add custom configured auto-scaling queues
        for queue_config in &self.queue_configs {
            if queue_config.auto_scaling {
                all_auto_queues.push(queue_config.name.clone());
            }
        }
        
        // Add queues from config that aren't already in custom configs
        for queue_name in &config_auto_queues {
            if !all_auto_queues.contains(queue_name) {
                all_auto_queues.push(queue_name.clone());
            }
        }
        
        if all_auto_queues.is_empty() {
            return Ok(HashMap::new());
        }

        info!(
            "📊 Setting up {} auto-scaling queues",
            all_auto_queues.len()
        );

        for queue_name in &all_auto_queues {
            if let Some(worker_range) = config.get_queue_worker_range(queue_name) {
                info!(
                    "🏊 Creating consumer pool for '{}' (workers: {}-{})",
                    queue_name, worker_range.min, worker_range.max
                );

                // Use custom handler if provided, otherwise skip this queue
                let handler = if let Some(custom_handler) = self.auto_scale_handlers.get(queue_name) {
                    self.create_custom_queue_message_handler(queue_name.clone(), custom_handler.clone())
                } else {
                    info!("No custom handler configured for auto-scaling queue '{}', skipping", queue_name);
                    continue;
                };
                
                // Create consumer pool
                let mut pool = ConsumerPool::new(
                    queue_name.clone(),
                    config.clone(),
                    worker_range.clone(),
                    handler,
                    shutdown_tx.subscribe(),
                );

                // Initialize pool with minimum workers
                pool.initialize().await?;
                
                consumer_pools.insert(queue_name.clone(), pool);
            }
        }

        Ok(consumer_pools)
    }

    /// Setup topic subscribers with custom handlers for auto-scaling mode
    async fn setup_custom_topic_subscribers(
        &self,
        config: &Config,
        shutdown_tx: &broadcast::Sender<()>,
    ) -> JoinSet<Result<()>> {
        let mut topic_tasks = JoinSet::new();
        
        // Get topic names from config
        let config_topics = config.get_all_topic_names();
        
        // Combine custom topics and config topics
        let mut all_topics = Vec::new();
        
        // Add custom configured topics
        for topic_config in &self.topic_configs {
            all_topics.push(topic_config.name.clone());
        }
        
        // Add topics from config that aren't already in custom configs
        for topic_name in &config_topics {
            if !all_topics.contains(topic_name) {
                all_topics.push(topic_name.clone());
            }
        }
        
        for topic_name in all_topics {
            info!(
                "📊 Topic '{}' configured with 1 static worker",
                topic_name
            );

            let config_clone = config.clone();
            let _shutdown_rx = shutdown_tx.subscribe();
            let topic_name_clone = topic_name.clone();
            
            // Find custom handler for this topic
            let custom_handler = self.topic_configs.iter()
                .find(|tc| tc.name == topic_name)
                .and_then(|tc| tc.handler.as_ref())
                .cloned();

            topic_tasks.spawn(async move {
                let mut service = StompService::new(config_clone).await?;
                
                if let Some(handler) = custom_handler {
                    service.receive_topic(&topic_name_clone, move |msg| handler(msg)).await
                } else {
                    // No handler configured - skip this topic
                    info!("No handler configured for topic '{}', skipping", topic_name_clone);
                    Ok(())
                }
            });
        }

        topic_tasks
    }

    /// Setup static subscribers with custom handlers for queues and topics
    async fn setup_custom_static_subscribers(
        &self,
        config: &Config,
        shutdown_tx: &broadcast::Sender<()>,
        subscriber_tasks: &mut JoinSet<Result<()>>,
    ) {
        // Get queue and topic names from config
        let config_queues = config.get_all_queue_names();
        let config_topics = config.get_all_topic_names();
        
        // Combine custom queues and config queues (excluding auto-scaling ones)
        let mut all_queues = Vec::new();
        
        // Add custom configured static queues
        for queue_config in &self.queue_configs {
            if !queue_config.auto_scaling {
                all_queues.push(queue_config.name.clone());
            }
        }
        
        // Add queues from config that aren't already in custom configs
        for queue_name in &config_queues {
            if !all_queues.contains(queue_name) && !config.get_auto_scaling_queues().contains(queue_name) {
                all_queues.push(queue_name.clone());
            }
        }
        
        // Combine custom topics and config topics
        let mut all_topics = Vec::new();
        
        // Add custom configured topics
        for topic_config in &self.topic_configs {
            all_topics.push(topic_config.name.clone());
        }
        
        // Add topics from config that aren't already in custom configs
        for topic_name in &config_topics {
            if !all_topics.contains(topic_name) {
                all_topics.push(topic_name.clone());
            }
        }

        info!(
            "🔧 Setting up {} queues and {} topics with static workers...",
            all_queues.len(),
            all_topics.len()
        );

        // Start subscribers for each queue
        for queue_name in all_queues {
            info!(
                "📊 Queue '{}' configured with 1 static worker",
                queue_name
            );

            let config_clone = config.clone();
            let _shutdown_rx = shutdown_tx.subscribe();
            let queue_name_clone = queue_name.clone();
            
            // Find custom handler for this queue
            let custom_handler = self.queue_configs.iter()
                .find(|qc| qc.name == queue_name && !qc.auto_scaling)
                .and_then(|qc| qc.handler.as_ref())
                .cloned();

            subscriber_tasks.spawn(async move {
                let mut service = StompService::new(config_clone).await?;
                
                if let Some(handler) = custom_handler {
                    service.receive_queue(&queue_name_clone, move |msg| handler(msg)).await
                } else {
                    // No handler configured - skip this queue
                    info!("No handler configured for queue '{}', skipping", queue_name_clone);
                    Ok(())
                }
            });
        }

        // Start subscribers for each topic
        for topic_name in all_topics {
            info!(
                "📊 Topic '{}' configured with 1 static worker",
                topic_name
            );

            let config_clone = config.clone();
            let _shutdown_rx = shutdown_tx.subscribe();
            let topic_name_clone = topic_name.clone();
            
            // Find custom handler for this topic
            let custom_handler = self.topic_configs.iter()
                .find(|tc| tc.name == topic_name)
                .and_then(|tc| tc.handler.as_ref())
                .cloned();

            subscriber_tasks.spawn(async move {
                let mut service = StompService::new(config_clone).await?;
                
                if let Some(handler) = custom_handler {
                    service.receive_topic(&topic_name_clone, move |msg| handler(msg)).await
                } else {
                    // No handler configured - skip this topic
                    info!("No handler configured for topic '{}', skipping", topic_name_clone);
                    Ok(())
                }
            });
        }
    }

    /// Setup shutdown signal handler
    fn setup_shutdown_handler(&self, shutdown_tx: broadcast::Sender<()>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            crate::utils::setup_signal_handlers().await;
            let _ = shutdown_tx.send(()); // Notify all services to shutdown
        })
    }

    /// Start autoscaler in background
    async fn start_autoscaler(&self, mut autoscaler: AutoScaler) -> tokio::task::JoinHandle<AutoScaler> {
        tokio::spawn(async move {
            if let Err(e) = autoscaler.run().await {
                error!("Auto-scaler failed: {}", e);
            }
            autoscaler
        })
    }

    /// Handle shutdown for auto-scaling system
    async fn shutdown_autoscaling_system(
        &self,
        shutdown_handle: tokio::task::JoinHandle<()>,
        autoscaler_handle: tokio::task::JoinHandle<AutoScaler>,
        mut topic_tasks: JoinSet<Result<()>>,
    ) -> Result<()> {
        // Wait for shutdown signal
        let _ = shutdown_handle.await;

        info!("🛑 Shutdown signal received, stopping auto-scaling system...");

        // Stop the auto-scaler and get it back
        let autoscaler = match autoscaler_handle.await {
            Ok(autoscaler) => autoscaler,
            Err(e) => {
                error!("Failed to join auto-scaler task: {}", e);
                return Ok(());
            }
        };

        // Stop all consumer pools
        autoscaler.stop_all_pools().await?;

        // Stop topic tasks
        topic_tasks.abort_all();
        while let Some(result) = topic_tasks.join_next().await {
            if let Err(e) = result {
                if !e.is_cancelled() {
                    warn!("Topic task error during shutdown: {}", e);
                }
            }
        }

        info!("✅ Auto-scaling application shutdown complete");
        Ok(())
    }

    /// Handle shutdown for static workers
    async fn shutdown_static_workers(
        &self,
        mut subscriber_tasks: JoinSet<Result<()>>,
        config: &Config,
    ) {
        let shutdown_timeout = Duration::from_secs(config.shutdown.timeout_secs as u64);
        let mut shutdown_count = 0;
        let total_subscribers = subscriber_tasks.len();

        let shutdown_start = tokio::time::Instant::now();
        while !subscriber_tasks.is_empty() && shutdown_start.elapsed() < shutdown_timeout {
            match tokio::time::timeout(Duration::from_secs(1), subscriber_tasks.join_next()).await {
                Ok(Some(result)) => {
                    shutdown_count += 1;
                    match result {
                        Ok(Ok(())) => {
                            info!(
                                "✅ Worker {}/{} shut down gracefully",
                                shutdown_count, total_subscribers
                            );
                        }
                        Ok(Err(e)) => {
                            warn!(
                                "⚠️ Worker {}/{} shut down with error: {}",
                                shutdown_count, total_subscribers, e
                            );
                        }
                        Err(e) => {
                            warn!(
                                "⚠️ Worker {}/{} join error: {}",
                                shutdown_count, total_subscribers, e
                            );
                        }
                    }
                }
                Ok(None) => break,
                Err(_) => continue,
            }
        }

        // Force stop remaining tasks
        if !subscriber_tasks.is_empty() {
            warn!(
                "⏰ Shutdown timeout reached, force stopping {} remaining workers",
                subscriber_tasks.len()
            );
            subscriber_tasks.abort_all();
            while let Some(result) = subscriber_tasks.join_next().await {
                if let Err(e) = result {
                    if !e.is_cancelled() {
                        warn!("Force-stopped worker error: {}", e);
                    }
                }
            }
        }

        info!(
            "✅ Static worker application shutdown complete ({} workers stopped)",
            total_subscribers
        );
    }

    /// Create a custom queue message handler for auto-scaling pools
    fn create_custom_queue_message_handler(
        &self,
        queue_name: String,
        custom_handler: AutoScaleHandlerFn,
    ) -> Box<MessageHandler> {
        let queue_name_clone = queue_name.clone();
        // Create a wrapper that calls the custom handler
        Box::new(move |msg: String| {
            let queue_name = queue_name_clone.clone();
            let handler = custom_handler.clone();
            Box::pin(async move {
                let start_time = std::time::Instant::now();
                info!(
                    "[{}] Processing message with custom handler: {}",
                    queue_name,
                    msg.chars().take(50).collect::<String>()
                );

                // Call the custom handler function
                let result = handler(msg).await;

                let processing_time = start_time.elapsed();
                match &result {
                    Ok(()) => {
                        info!(
                            "[{}] ✅ Message processed successfully in {}ms",
                            queue_name,
                            processing_time.as_millis()
                        );
                    }
                    Err(e) => {
                        error!("[{}] ❌ Message processing failed: {}", queue_name, e);
                    }
                }
                result
            }) as Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
        })
    }
}
