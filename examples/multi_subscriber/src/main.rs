use stomp::session_builder::SessionBuilder;
use stomp::session::SessionEvent;
use stomp::subscription::{AckOrNack, AckMode};
use stomp::connection::{HeartBeat, Credentials};
use stomp::header::Header;
use futures::{StreamExt, pin_mut};
use std::time::{Duration, Instant};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicBool, Ordering};
use clap::Parser;
use uuid::Uuid;
use dashmap::DashMap;
use tracing::{info, warn, debug, error, trace};
use tokio::signal;

#[derive(Parser)]
#[command(about = "Multi-subscriber STOMP consumer")]
struct Args {
    /// Number of concurrent subscribers
    #[arg(short, long, default_value = "3")]
    subscribers: u32,
    
    /// Queue to consume from
    #[arg(short, long, default_value = "/queue/test")]
    queue: String,
    
    /// Processing delay in milliseconds
    #[arg(short, long, default_value = "100")]
    delay_ms: u64,
}

#[derive(Debug)]
#[allow(dead_code)]
struct MessageTrackingInfo {
    first_seen_at: Instant,
    first_seen_by_worker: u32,
    ack_attempts: u32,
    last_ack_at: Option<Instant>,
    acked_by_worker: Option<u32>,
    is_duplicate_receive: bool,
    is_duplicate_ack: bool,
}

struct SharedStats {
    total_received: AtomicU32,
    total_acked: AtomicU32,
    total_failed: AtomicU32,
    total_duplicate_receives: AtomicU32,
    total_duplicate_acks: AtomicU32,
    global_ack_sequence: AtomicU64,
    start_time: Instant,
    // Graceful shutdown flag
    shutdown_requested: AtomicBool,
    // Message ID -> tracking info
    message_tracker: DashMap<String, MessageTrackingInfo>,
    // ACK sequence -> message info for debugging
    ack_audit_log: DashMap<u64, (String, u32, Instant)>, // (message_id, worker_id, timestamp)
}

impl SharedStats {
    fn new() -> Self {
        Self {
            total_received: AtomicU32::new(0),
            total_acked: AtomicU32::new(0),
            total_failed: AtomicU32::new(0),
            total_duplicate_receives: AtomicU32::new(0),
            total_duplicate_acks: AtomicU32::new(0),
            global_ack_sequence: AtomicU64::new(0),
            start_time: Instant::now(),
            shutdown_requested: AtomicBool::new(false),
            message_tracker: DashMap::new(),
            ack_audit_log: DashMap::new(),
        }
    }
    
    /// Track a received message and detect duplicates
    fn track_message_received(&self, message_id: &str, worker_id: u32) -> bool {
        let now = Instant::now();
        let mut is_duplicate = false;
        
        self.message_tracker.entry(message_id.to_string())
            .and_modify(|info| {
                info.is_duplicate_receive = true;
                is_duplicate = true;
                warn!("🔄 Duplicate receive: message_id={}, original_worker={}, new_worker={}", 
                     message_id, info.first_seen_by_worker, worker_id);
            })
            .or_insert_with(|| {
                MessageTrackingInfo {
                    first_seen_at: now,
                    first_seen_by_worker: worker_id,
                    ack_attempts: 0,
                    last_ack_at: None,
                    acked_by_worker: None,
                    is_duplicate_receive: false,
                    is_duplicate_ack: false,
                }
            });
            
        if is_duplicate {
            self.total_duplicate_receives.fetch_add(1, Ordering::Relaxed);
        }
        self.total_received.fetch_add(1, Ordering::Relaxed);
        
        is_duplicate
    }
    
    /// Try to claim ownership of ACKing this message (atomic operation)
    fn try_claim_ack(&self, message_id: &str, worker_id: u32) -> bool {
        if let Some(mut info) = self.message_tracker.get_mut(message_id) {
            if info.acked_by_worker.is_none() {
                // Successfully claim ACK ownership
                info.acked_by_worker = Some(worker_id);
                info.last_ack_at = Some(Instant::now());
                true
            } else {
                // Already claimed by another worker
                false
            }
        } else {
            // Message not found
            false
        }
    }
    
    /// Track an ACK attempt and detect duplicates
    fn track_message_acked(&self, message_id: &str, worker_id: u32) -> (u64, bool) {
        let ack_sequence = self.global_ack_sequence.fetch_add(1, Ordering::SeqCst);
        let now = Instant::now();
        let mut is_duplicate_ack = false;
        
        // Record in audit log
        self.ack_audit_log.insert(ack_sequence, (message_id.to_string(), worker_id, now));
        
        // Update message tracking
        if let Some(mut info) = self.message_tracker.get_mut(message_id) {
            info.ack_attempts += 1;
            
            // Check if this is already a duplicate (should not happen with new logic)
            if info.acked_by_worker.is_some() && info.acked_by_worker != Some(worker_id) {
                info.is_duplicate_ack = true;
                is_duplicate_ack = true;
                self.total_duplicate_acks.fetch_add(1, Ordering::Relaxed);
                
                error!("⚠️  Duplicate ACK detected: message_id={}, sequence={}, original_worker={:?}, new_worker={}",
                      message_id, ack_sequence, info.acked_by_worker, worker_id);
            }
            // Note: acked_by_worker should already be set by try_claim_ack()
        } else {
            warn!("❓ ACKing unknown message: message_id={}, worker_id={}, sequence={}", 
                 message_id, worker_id, ack_sequence);
        }
        
        if !is_duplicate_ack {
            self.total_acked.fetch_add(1, Ordering::Relaxed);
        }
        
        (ack_sequence, is_duplicate_ack)
    }
    
    /// Request graceful shutdown
    fn request_shutdown(&self) {
        self.shutdown_requested.store(true, Ordering::Relaxed);
        info!("🛑 Graceful shutdown requested - workers will finish current messages and stop");
    }
    
    /// Check if shutdown has been requested
    fn is_shutdown_requested(&self) -> bool {
        self.shutdown_requested.load(Ordering::Relaxed)
    }
    
    fn print_summary(&self) {
        let received = self.total_received.load(Ordering::Relaxed);
        let acked = self.total_acked.load(Ordering::Relaxed);
        let failed = self.total_failed.load(Ordering::Relaxed);
        let dup_receives = self.total_duplicate_receives.load(Ordering::Relaxed);
        let dup_acks = self.total_duplicate_acks.load(Ordering::Relaxed);
        let elapsed = self.start_time.elapsed().as_secs_f64();
        let rate = if elapsed > 0.0 { received as f64 / elapsed } else { 0.0 };
        
        println!("📊 TOTAL: Received={} ({}🔄), ACK'd={} ({}⚠️), Failed={}, Rate={:.1}/s", 
                received, dup_receives, acked, dup_acks, failed, rate);
    }
    
    fn print_detailed_report(&self) {
        let received = self.total_received.load(Ordering::Relaxed);
        let acked = self.total_acked.load(Ordering::Relaxed);
        let failed = self.total_failed.load(Ordering::Relaxed);
        let dup_receives = self.total_duplicate_receives.load(Ordering::Relaxed);
        let dup_acks = self.total_duplicate_acks.load(Ordering::Relaxed);
        let unique_messages = self.message_tracker.len();
        let total_ack_sequences = self.ack_audit_log.len();
        
        println!("\n🔍 DETAILED ANALYSIS:");
        println!("   📨 Total message receives:    {} (unique: {})", received, unique_messages);
        println!("   🔄 Duplicate receives:        {} ({:.2}%)", dup_receives, 
                if received > 0 { dup_receives as f64 / received as f64 * 100.0 } else { 0.0 });
        println!("   ✅ Total ACK attempts:        {} (sequences: {})", acked + dup_acks, total_ack_sequences);
        println!("   ⚠️  Duplicate ACK attempts:    {} ({:.2}%)", dup_acks,
                if total_ack_sequences > 0 { dup_acks as f64 / total_ack_sequences as f64 * 100.0 } else { 0.0 });
        println!("   ❌ Failed ACK attempts:       {}", failed);
        println!("   📊 Net ACKs sent to broker:   {}", acked);
        println!("   🧮 Expected broker count:     {} messages acknowledged", acked);
        
        // Check for unacked messages
        let unacked_count = self.message_tracker.iter()
            .filter(|entry| entry.value().acked_by_worker.is_none())
            .count();
        if unacked_count > 0 {
            warn!("⚠️  {} messages received but never ACKed", unacked_count);
        }
        
        // Check for potential issues
        let discrepancy = received as i32 - acked as i32 - failed as i32;
        if discrepancy != 0 {
            error!("❗ DISCREPANCY DETECTED: Received({}) - Acked({}) - Failed({}) = {}", 
                  received, acked, failed, discrepancy);
        }
    }
}

async fn consumer_worker(
    worker_id: u32,
    queue: String,
    delay_ms: u64,
    shared_stats: Arc<SharedStats>,
) -> Result<(), Box<dyn std::error::Error>> {
    let client_id = format!("consumer-{}", worker_id);
    let unique_id = Uuid::new_v4().to_string().split('-').next().unwrap().to_string();
    
    info!("🚀 [Worker-{}] Starting consumer with ID: {}-{}", worker_id, client_id, unique_id);
    
    // Connect to ActiveMQ
    let session = SessionBuilder::new("10.0.7.127", 31333)
        .with(HeartBeat(30_000, 30_000))
        .with(Credentials("admin", "admin"))
        .with(Header::new("client-id", &format!("{}-{}", client_id, unique_id)))
        .start()
        .await?;
    
    pin_mut!(session);
    
    let mut connected = false;
    let mut local_received = 0u32;
    let mut local_acked = 0u32;
    let mut local_failed = 0u32;
    let mut last_message_time = Instant::now();
    
    let no_message_timeout = Duration::from_secs(15);
    
    while let Some(event) = session.next().await {
        match event {
            SessionEvent::Connected => {
                info!("✅ [Worker-{}] Connected to STOMP server!", worker_id);
                
                // Subscribe with CLIENT-INDIVIDUAL ACK mode for precise ACK control
                let queue_sub = session
                    .as_mut()
                    .subscription(&queue)
                    .with(AckMode::ClientIndividual) // Changed from Client to ClientIndividual
                    .start()
                    .await?;
                
                info!("📬 [Worker-{}] Subscribed to {} with ID: {} (ACK mode: individual)", worker_id, queue, queue_sub);
                connected = true;
                last_message_time = Instant::now();
            }

            SessionEvent::Message { destination: _, frame, .. } => {
                local_received += 1;
                last_message_time = Instant::now();
                
                // Parse message body
                let body_str = std::str::from_utf8(&frame.body)
                    .unwrap_or("<non-UTF8>");

                // Extract full message ID
                let message_id = frame.headers.headers.iter()
                    .find(|h| h.0 == "message-id")
                    .map(|h| h.1.as_str())
                    .unwrap_or("unknown")
                    .to_string();

                // Track message and detect duplicates
                let is_duplicate = shared_stats.track_message_received(&message_id, worker_id);
                
                let display_id = message_id.split(':').last().unwrap_or(&message_id);
                
                if is_duplicate {
                    warn!("📨 [Worker-{}] DUPLICATE Message #{}: '{}' (ID: {})", 
                          worker_id,
                          local_received, 
                          body_str.chars().take(30).collect::<String>(),
                          display_id);
                } else {
                    debug!("📨 [Worker-{}] Message #{}: '{}' (ID: {})", 
                          worker_id,
                          local_received, 
                          body_str.chars().take(30).collect::<String>(),
                          display_id);
                }

                // Simulate processing delay
                if delay_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }

                // IDEMPOTENT ACKNOWLEDGMENT - Check before ACKing
                trace!("   🔄 [Worker-{}] Checking if message {} can be ACKed", worker_id, display_id);
                
                // Try to claim ACK ownership atomically
                if shared_stats.try_claim_ack(&message_id, worker_id) {
                    // Successfully claimed - now safe to ACK to broker
                    trace!("   ✓ [Worker-{}] Claimed ACK ownership for message {}", worker_id, display_id);
                    
                    match session.as_mut().acknowledge_frame(&frame, AckOrNack::Ack).await {
                        Ok(()) => {
                            local_acked += 1;
                            let (ack_sequence, is_duplicate_ack) = shared_stats.track_message_acked(&message_id, worker_id);
                            
                            if is_duplicate_ack {
                                error!("   ⚠️  [Worker-{}] UNEXPECTED DUPLICATE ACK seq#{} for message {}", worker_id, ack_sequence, display_id);
                            } else {
                                info!("   ✓ [Worker-{}] ACK SUCCESS seq#{} for message {}", worker_id, ack_sequence, display_id);
                            }
                        }
                        Err(e) => {
                            local_failed += 1;
                            shared_stats.total_failed.fetch_add(1, Ordering::Relaxed);
                            error!("   ❌ [Worker-{}] ACK FAILED for message {}: {}", worker_id, display_id, e);
                            
                            // Reset the ACK claim since it failed
                            if let Some(mut info) = shared_stats.message_tracker.get_mut(&message_id) {
                                info.acked_by_worker = None;
                                info.last_ack_at = None;
                            }
                        }
                    }
                } else {
                    // Message already claimed by another worker - skip ACK
                    warn!("   ⏭️  [Worker-{}] Message {} already ACKed by another worker - SKIPPING", worker_id, display_id);
                    
                    // Still track this as a prevented duplicate
                    shared_stats.total_duplicate_acks.fetch_add(1, Ordering::Relaxed);
                }

                // Show worker progress periodically
                if local_received % 10 == 0 {
                    info!("📈 [Worker-{}] Local progress: Received={}, ACKed={}, Failed={}", 
                         worker_id, local_received, local_acked, local_failed);
                    shared_stats.print_summary();
                    
                    // Print detailed report every 50 messages
                    if local_received % 50 == 0 {
                        shared_stats.print_detailed_report();
                    }
                }
            }

            SessionEvent::Receipt { id, .. } => {
                debug!("📄 [Worker-{}] Receipt: {}", worker_id, id);
            }

            SessionEvent::ErrorFrame(frame) => {
                error!("❌ [Worker-{}] Error frame: {:?}", worker_id, frame);
                break;
            }

            SessionEvent::Disconnected(reason) => {
                warn!("🔌 [Worker-{}] Session disconnected: {:?}", worker_id, reason);
                break;
            }

            _ => {
                // Ignore other events
            }
        }
        
        // Check for graceful shutdown request
        if shared_stats.is_shutdown_requested() {
            info!("🛑 [Worker-{}] Shutdown requested - stopping gracefully", worker_id);
            break;
        }
        
        // Check for timeout (no messages received)
        if connected && last_message_time.elapsed() > no_message_timeout {
            info!("⏰ [Worker-{}] No messages for {}s - stopping gracefully", 
                 worker_id, no_message_timeout.as_secs());
            break;
        }
    }

    // Final worker statistics
    info!("🏁 [Worker-{}] Final Stats: Received={}, ACKed={}, Failed={}", 
         worker_id, local_received, local_acked, local_failed);

    // Graceful disconnect
    if let Err(e) = session.as_mut().disconnect().await {
        warn!("⚠️  [Worker-{}] Disconnect error: {}", worker_id, e);
    } else {
        info!("👋 [Worker-{}] Disconnected gracefully", worker_id);
    }
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize structured logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into())
        )
        .with_target(false)
        .with_thread_ids(true)
        .with_line_number(true)
        .init();
    
    let args = Args::parse();
    
    println!("🔍 Multi-Subscriber STOMP Consumer");
    println!("📋 Configuration:");
    println!("   • Subscribers: {}", args.subscribers);
    println!("   • Queue: {}", args.queue);
    println!("   • Processing delay: {}ms", args.delay_ms);
    println!("   • ActiveMQ: 10.0.7.127:31333");
    println!();

    let shared_stats = Arc::new(SharedStats::new());
    let mut handles = Vec::new();

    // Setup signal handler for graceful shutdown
    let stats_for_signal = Arc::clone(&shared_stats);
    let signal_handle = tokio::spawn(async move {
        #[cfg(unix)]
        {
            let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate()).expect("Failed to register SIGTERM handler");
            let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt()).expect("Failed to register SIGINT handler");
            
            tokio::select! {
                _ = sigterm.recv() => {
                    info!("📡 Received SIGTERM - initiating graceful shutdown");
                    stats_for_signal.request_shutdown();
                }
                _ = sigint.recv() => {
                    info!("📡 Received SIGINT (Ctrl+C) - initiating graceful shutdown");
                    stats_for_signal.request_shutdown();
                }
            }
        }
        
        #[cfg(windows)]
        {
            match signal::ctrl_c().await {
                Ok(()) => {
                    info!("📡 Received Ctrl+C - initiating graceful shutdown");
                    stats_for_signal.request_shutdown();
                }
                Err(err) => {
                    error!("Unable to listen for shutdown signal: {}", err);
                }
            }
        }
    });

    // Launch multiple consumer workers
    for worker_id in 1..=args.subscribers {
        let queue_clone = args.queue.clone();
        let stats_clone = Arc::clone(&shared_stats);
        
        let handle = tokio::spawn(async move {
            if let Err(e) = consumer_worker(worker_id, queue_clone, args.delay_ms, stats_clone).await {
                error!("❌ [Worker-{}] Error: {}", worker_id, e);
            }
        });
        
        handles.push(handle);
        
        // Small delay between connections to avoid overwhelming ActiveMQ
        // tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Wait for all workers to complete or signal handler
    info!("⏳ Waiting for all {} consumers to finish... (Press Ctrl+C for graceful shutdown)", args.subscribers);
    
    // Collect all handles including signal handler
    handles.push(signal_handle);
    
    // Wait for any task to complete
    let (finished_result, _index, remaining) = futures::future::select_all(handles).await;
    
    // If signal handler finished, request shutdown for remaining workers
    if shared_stats.is_shutdown_requested() {
        info!("📋 Shutdown signal received, waiting for workers to finish gracefully...");
        
        // Give workers some time to finish current messages
        let shutdown_timeout = Duration::from_secs(10);
        let start_shutdown = Instant::now();
        
        // Wait for remaining workers with timeout
        for (i, handle) in remaining.into_iter().enumerate() {
            if start_shutdown.elapsed() > shutdown_timeout {
                warn!("⏰ Shutdown timeout reached, force terminating remaining workers");
                handle.abort();
                continue;
            }
            
            match tokio::time::timeout(
                shutdown_timeout - start_shutdown.elapsed(),
                handle
            ).await {
                Ok(result) => {
                    if let Err(e) = result {
                        if !e.is_cancelled() {
                            error!("❌ Worker {} shutdown error: {}", i + 1, e);
                        }
                    }
                }
                Err(_) => {
                    warn!("⏰ Worker {} shutdown timeout, force terminating", i + 1);
                    // Handle is automatically dropped and cancelled
                }
            }
        }
    } else {
        // Normal completion case
        if let Err(e) = finished_result {
            error!("❌ Task completion error: {}", e);
        }
        
        // Wait for any remaining tasks
        for (i, handle) in remaining.into_iter().enumerate() {
            if let Err(e) = handle.await {
                if !e.is_cancelled() {
                    error!("❌ Task {} completion error: {}", i + 1, e);
                }
            }
        }
    }

    // Final comprehensive analysis
    let total_time = shared_stats.start_time.elapsed().as_secs_f64();
    let total_received = shared_stats.total_received.load(Ordering::Relaxed);
    let total_acked = shared_stats.total_acked.load(Ordering::Relaxed);
    let total_failed = shared_stats.total_failed.load(Ordering::Relaxed);
    let total_dup_receives = shared_stats.total_duplicate_receives.load(Ordering::Relaxed);
    let total_dup_acks = shared_stats.total_duplicate_acks.load(Ordering::Relaxed);
    let unique_messages = shared_stats.message_tracker.len();
    let total_ack_sequences = shared_stats.ack_audit_log.len();
    
    println!();
    if shared_stats.is_shutdown_requested() {
        println!("🛑 Multi-Subscriber Test Gracefully Stopped!");
    } else {
        println!("🎉 Multi-Subscriber Test Complete!");
    }
    println!("📊 FINAL COMPREHENSIVE REPORT:");
    println!("   👥 Subscribers used:        {}", args.subscribers);
    println!("   ⏱️  Total time:              {:.2}s", total_time);
    println!("   📨 Total receives:          {} (unique: {})", total_received, unique_messages);
    println!("   🔄 Duplicate receives:      {} ({:.2}%)", total_dup_receives,
            if total_received > 0 { total_dup_receives as f64 / total_received as f64 * 100.0 } else { 0.0 });
        println!("   ✔️ Successful ACKs:         {}", total_acked);
        println!("   ⚠️  Prevented duplicate ACKs: {} ({:.2}%)", total_dup_acks,
                if total_received > 0 { total_dup_acks as f64 / total_received as f64 * 100.0 } else { 0.0 });
        println!("   ❌ Failed ACKs:             {}", total_failed);
    println!("   📊 Total ACK sequences:     {}", total_ack_sequences);
    
    if total_time > 0.0 {
        println!("   📈 Combined throughput:     {:.1} msg/sec", total_received as f64 / total_time);
        println!("   📈 Per-subscriber average:  {:.1} msg/sec", 
                (total_received as f64 / total_time) / args.subscribers as f64);
        println!("   📈 ACK rate:                {:.1} ack/sec", total_acked as f64 / total_time);
    }
    
    // Final detailed analysis
    shared_stats.print_detailed_report();
    
    // Health check
    println!();
    let perfect_score = total_acked == unique_messages as u32 && total_dup_acks == 0 && total_failed == 0;
    
    if perfect_score {
        info!("🎉 PERFECT SCORE! All {} unique messages successfully ACKed with no duplicates!", unique_messages);
    } else {
        let expected_acks = unique_messages as u32;
        let actual_clean_acks = total_acked;
        let discrepancy = expected_acks as i32 - actual_clean_acks as i32;
        
        if discrepancy != 0 {
            error!("⚠️  DISCREPANCY FOUND: Expected {} ACKs, got {} clean ACKs (difference: {})", 
                  expected_acks, actual_clean_acks, discrepancy);
            error!("🔍 This may indicate the '5 queue berlebih' issue you mentioned!");
        }
        
        if total_dup_acks > 0 {
            info!("🛑 {} duplicate ACKs were PREVENTED from reaching broker (this is good!)", total_dup_acks);
        }
        
        if total_failed > 0 {
            warn!("⚠️  {} ACK attempts failed - some messages may not be acknowledged", total_failed);
        }
    }
    
    // Save audit trail for further analysis
    let audit_data = serde_json::json!({
        "test_config": {
            "subscribers": args.subscribers,
            "queue": args.queue,
            "delay_ms": args.delay_ms,
            "total_time_seconds": total_time
        },
        "final_stats": {
            "total_received": total_received,
            "unique_messages": unique_messages,
            "duplicate_receives": total_dup_receives,
            "successful_acks": total_acked,
            "duplicate_acks": total_dup_acks,
            "failed_acks": total_failed,
            "total_ack_sequences": total_ack_sequences,
            "discrepancy": unique_messages as i32 - total_acked as i32
        }
    });
    
    println!();
    println!("📄 Audit data (for troubleshooting):");
    println!("{}", serde_json::to_string_pretty(&audit_data).unwrap_or_else(|_| "Failed to serialize".to_string()));

    Ok(())
}