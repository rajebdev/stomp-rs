# STOMP Service Application

A complete Rust application implementing STOMP (Simple Text Orientated Messaging Protocol) functionality with topic and queue messaging capabilities.

## Architecture

The application follows a modular architecture with the following components:

### Core Modules

- **`main.rs`** - Application entry point with async runtime and graceful shutdown
- **`config.rs`** - Configuration management using YAML files
- **`service.rs`** - Core STOMP service with send/receive operations
- **`handler.rs`** - Message handlers for processing topic and queue messages

### Features

✅ **Complete STOMP Integration**
- Send messages to topics and queues
- Receive messages with custom handlers
- Individual message acknowledgment
- Graceful connection management

✅ **Configuration Management**
- YAML-based configuration
- Environment variable overrides
- Broker connection settings
- Destination management

✅ **Robust Message Handling**
- Async/await patterns throughout
- Proper error handling and propagation
- Structured logging with tracing
- Message processing metrics

✅ **Production Ready**
- Graceful shutdown on SIGTERM/SIGINT
- Connection retry logic
- Heartbeat configuration
- Timeout handling

## Quick Start

### Prerequisites

- Rust 1.70.0 or later
- A running STOMP broker (ActiveMQ, RabbitMQ, etc.)

### Configuration

Update `config.yaml` with your broker details:

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
  # ... additional configuration
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
🚀 Starting STOMP Application: stomp-service
📋 Version: 1.0.0
🔗 Broker: 10.0.7.127:31333
📤 Sending test messages...
✅ Test messages sent successfully
📥 Starting topic consumer...
📥 Starting queue consumer...
🔄 Application running... Press Ctrl+C to shutdown gracefully
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