# STOMP Multi-Subscriber Application

A high-performance Rust application implementing STOMP (Simple Text Orientated Messaging Protocol) with **multi-subscriber architecture** for concurrent message processing across topics and queues.

## Architecture

The application follows a modular architecture with the following components:

### Core Modules

- **`main.rs`** - Application entry point with async runtime and graceful shutdown
- **`config.rs`** - Configuration management using YAML files
- **`service.rs`** - Core STOMP service with send/receive operations
- **`handler.rs`** - Message handlers for processing topic and queue messages

### Features

✅ **Multi-Subscriber Architecture** ⭐ **NEW**
- Configure multiple concurrent subscribers per destination
- Load distribution for queues (competing consumers)
- Broadcasting for topics (all subscribers receive messages)
- Individual connection management per subscriber
- Subscriber-specific logging and monitoring

✅ **Complete STOMP Integration**
- Send messages to topics and queues
- Receive messages with custom handlers
- Individual message acknowledgment
- Graceful connection management

✅ **Configuration Management**
- YAML-based configuration with `workers_per_queue` and `workers_per_topic`
- Environment variable overrides
- Broker connection settings
- Destination management

✅ **Robust Message Handling**
- Async/await patterns throughout
- Proper error handling and propagation
- Structured logging with tracing
- Message processing metrics

✅ **Production Ready**
- Coordinated graceful shutdown for all subscribers
- Connection retry logic
- Heartbeat configuration
- Timeout handling

## Quick Start

### Prerequisites

- Rust 1.70.0 or later
- A running STOMP broker (ActiveMQ, RabbitMQ, etc.)

### Configuration

Update `config.yaml` with your broker details and **multi-subscriber settings**:

```yaml
service:
  name: "stomp-service"
  version: "1.0.0"
  description: "Production STOMP messaging service"

broker:
  host: "10.0.7.127"  # Your STOMP broker IP
  port: 31333         # Your STOMP broker port
  credentials:
    username: "admin"
    password: "admin"
  heartbeat:
    client_send_secs: 10000
    client_receive_secs: 10000

# ⭐ NEW: Multi-subscriber configuration
consumers:
  ack_mode: "client_individual"
  
  # Number of concurrent workers per queue (load distribution)
  workers_per_queue:
    default: 2         # 2 workers for default queue
    api_requests: 4    # 4 workers for high-volume API requests  
    errors: 1          # 1 worker for error queue
  
  # Number of concurrent workers per topic (broadcasting)
  workers_per_topic:
    notifications: 2   # 2 workers for notifications topic
    events: 1          # 1 worker for events topic
```

### Running the Application

```bash
# Build and run
cargo run

# Or build first, then run
cargo build --release
./target/release/stomp_app
```

### Example Output

```
🚀 Starting Multi-Subscriber STOMP Application: stomp-service
📋 Version: 1.0.0
🔗 Broker: 10.0.7.127:31333
📊 Queue 'default' configured with 2 workers
📊 Queue 'api_requests' configured with 4 workers
📊 Queue 'errors' configured with 1 workers
📊 Topic 'notifications' configured with 2 workers
📊 Topic 'events' configured with 1 workers
🔧 Spawning 10 total subscribers...
[default][default#1] Starting subscriber...
[default][default#2] Starting subscriber...
[api_requests][api_requests#1] Starting subscriber...
[notifications][notifications#1] Starting subscriber...
[notifications][notifications#2] Starting subscriber...
📤 Sending test messages to multiple subscribers...
✅ Test messages sent successfully
🔄 Multi-subscriber system running... Press Ctrl+C to shutdown gracefully
```

## Multi-Subscriber Behavior

### Queue Workers (Load Distribution)

With multiple subscribers on a **queue**, messages are **distributed** among workers:

```
Queue: api_requests (4 workers)
├─ api_requests#1 ← Message A
├─ api_requests#2 ← Message B  
├─ api_requests#3 ← Message C
└─ api_requests#4 ← Message D
```

**Use Cases:**
- High-throughput message processing
- Load balancing across multiple workers
- Parallel processing of independent tasks

### Topic Workers (Broadcasting)

With multiple subscribers on a **topic**, **all workers** receive each message:

```
Topic: notifications (2 workers)
├─ notifications#1 ← Message X (copy 1)
└─ notifications#2 ← Message X (copy 2)
```

**Use Cases:**
- Event broadcasting to multiple handlers
- Redundant processing for reliability
- Different processing logic per subscriber

### Subscriber Logging

Each subscriber logs with a unique identifier for easy tracking:

```
[api_requests][api_requests#2] Processing queue message: {"order_id": 12345}
[api_requests][api_requests#2] ✅ Message processed successfully in 45ms
[notifications][notifications#1] Processing topic message: Event broadcast
[notifications][notifications#2] Processing topic message: Event broadcast
```

## API Specification

### Service Methods

The `StompService` provides the following methods as specified:

```rust
// Constructor
pub async fn new(config: Config) -> Result<Self>

// Send message to topic
pub async fn send_topic(&mut self, topic_name: &str, payload: &str, headers: HashMap<String, String>) -> Result<()>

// Send message to queue
pub async fn send_queue(&mut self, queue_name: &str, payload: &str, headers: HashMap<String, String>) -> Result<()>

// Receive messages from topic with handler
pub async fn receive_topic<F>(&mut self, topic_name: &str, handler: F) -> Result<()>

// Receive messages from queue with handler
pub async fn receive_queue<F>(&mut self, queue_name: &str, handler: F) -> Result<()>
```

### Handler Methods

The `MessageHandlers` provides default handlers:

```rust
// Constructor
pub fn new() -> Self

// Topic message handler
pub async fn topic_handler(msg: String) -> Result<()>

// Queue message handler
pub async fn queue_handler(msg: String) -> Result<()>
```

## Usage Examples

### Sending Messages

```rust
let mut service = StompService::new(config).await?;

// Send to topic with headers
let mut headers = HashMap::new();
headers.insert("priority".to_string(), "high".to_string());
service.send_topic("notifications", "Hello Topic!", headers).await?;

// Send to queue
service.send_queue("default", "Hello Queue!", HashMap::new()).await?;
```

### Receiving Messages

```rust
// Listen to topic with custom handler
service.receive_topic("notifications", |msg| {
    Box::pin(async move {
        println!("Topic message: {}", msg);
        Ok(())
    })
}).await?;

// Listen to queue with default handler
service.receive_queue("default", |msg| {
    Box::pin(MessageHandlers::queue_handler(msg))
}).await?;
```

## Configuration Reference

### Broker Settings

```yaml
broker:
  host: "localhost"        # STOMP broker hostname
  port: 61613             # STOMP port (default: 61613)
  credentials:            # Optional authentication
    username: "admin"
    password: "admin"
  heartbeat:              # Connection heartbeat (milliseconds)
    client_send_secs: 10000
    client_receive_secs: 10000
  retry:                  # Connection retry settings
    max_attempts: 5
    initial_delay_secs: 1
    max_delay_secs: 60
    backoff_multiplier: 2.0
  headers: {}             # Custom connection headers
```

### Multi-Subscriber Settings ⭐ **NEW**

```yaml
consumers:
  ack_mode: "client_individual"  # Acknowledgment mode
  
  # Queue workers (competing consumers)
  workers_per_queue:
    high_volume: 8      # 8 workers for load balancing
    medium_load: 4      # 4 workers for moderate load
    low_priority: 1     # Single worker for low-priority tasks
    
  # Topic workers (broadcast receivers)  
  workers_per_topic:
    events: 3           # 3 workers for event processing
    notifications: 2    # 2 workers for notifications
    monitoring: 1       # Single worker for monitoring
```

### Destinations

```yaml
destinations:
  queues:
    default:
      path: "/queue/demo"
      headers: {}
    api_requests:
      path: "/queue/api.requests"
      headers:
        persistent: "true"
  
  topics:
    notifications:
      path: "/topic/notifications"
      headers: {}
    events:
      path: "/topic/events"
      headers:
        persistent: "false"
```

## Testing

Run the unit tests:

```bash
cargo test
```

Test with a real STOMP broker:

```bash
# Make sure ActiveMQ or another STOMP broker is running
# Update config.yaml with correct broker details
cargo run
```

## Dependencies

- **stomp** - STOMP 1.2 client library
- **tokio** - Async runtime
- **anyhow** - Error handling
- **serde/serde_yaml** - Configuration parsing
- **tracing** - Structured logging
- **futures** - Async utilities
- **uuid** - Unique ID generation
- **chrono** - Date/time handling

## Production Deployment

### Environment Variables

Override configuration with environment variables:

```bash
export STOMP_BROKER_HOST=production.broker.com
export STOMP_BROKER_PORT=61613
export STOMP_BROKER_USERNAME=prod_user
export STOMP_LOGGING_LEVEL=warn
```

### Docker

Create a `Dockerfile`:

```dockerfile
FROM rust:1.70-slim as builder
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=builder /target/release/stomp_app /usr/local/bin/
COPY config.yaml /app/
WORKDIR /app
CMD ["stomp_app"]
```

### Systemd Service

```ini
[Unit]
Description=STOMP Service
After=network.target

[Service]
Type=exec
User=stomp
ExecStart=/usr/local/bin/stomp_app
WorkingDirectory=/opt/stomp
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

## Troubleshooting

### Common Issues

1. **Connection refused**
   - Check broker is running and accessible
   - Verify host/port in config.yaml
   - Check firewall settings

2. **Authentication failures**
   - Verify credentials in config.yaml
   - Check broker user permissions

3. **Message not received**
   - Verify destination paths match broker configuration
   - Check queue vs topic usage
   - Ensure subscriber is connected before sending

### Debug Logging

Enable debug logging:

```bash
RUST_LOG=debug cargo run
```

Or set in config.yaml:
```yaml
logging:
  level: "debug"
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.