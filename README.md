# stomp-rs

A modern Rust implementation of the [STOMP 1.2 protocol](http://stomp.github.io/stomp-specification-1.2.html) with full async/await support.

[![Rust](https://img.shields.io/badge/rust-1.70%2B-blue.svg)](https://www.rust-lang.org)
[![Edition](https://img.shields.io/badge/edition-2021-blue.svg)](https://doc.rust-lang.org/edition-guide/rust-2021/index.html)

This library allows Rust programs to interact with message queueing services like [ActiveMQ](http://activemq.apache.org/) and [RabbitMQ](https://www.rabbitmq.com/) using the STOMP protocol.

## Features

- ✅ Full STOMP 1.2 protocol support
- ✅ Modern async/await API with tokio 1.x
- ✅ Connection management and heartbeats
- ✅ Message publishing and subscription
- ✅ Transaction support
- ✅ Receipt handling
- ✅ Custom headers and message properties
- ✅ Comprehensive error handling

## Requirements

- **Rust 1.70.0 or later**
- **Edition 2021**
- **tokio runtime** (async environment)

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
stomp = "0.13"
tokio = { version = "1", features = ["full"] }
futures = "0.3"  # for Stream processing
```

## Quick Start

Here's a simple example showing how to connect, subscribe, and send messages:

```rust
use stomp::session_builder::SessionBuilder;
use stomp::session::SessionEvent;
use stomp::subscription::AckOrNack;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create and start a session
    let mut session = SessionBuilder::new("127.0.0.1", 61613)
        .start()
        .await?;

    // Wait for connection
    while let Some(event) = session.next_event().await {
        match event {
            SessionEvent::Connected => {
                println!("Connected to STOMP server!");
                break;
            }
            SessionEvent::ErrorFrame(frame) => {
                eprintln!("Connection error: {:?}", frame);
                return Err("Connection failed".into());
            }
            _ => continue,
        }
    }

    // Subscribe to a destination
    let subscription_id = session
        .subscription("/queue/test")
        .start()
        .await?;

    println!("Subscribed with ID: {}", subscription_id);

    // Send a message
    session
        .message("/queue/test", "Hello, STOMP!")
        .send()
        .await?;

    // Process incoming messages
    while let Some(event) = session.next_event().await {
        match event {
            SessionEvent::Message { frame, .. } => {
                let body = std::str::from_utf8(&frame.body)?;
                println!("Received: {}", body);
                
                // Acknowledge the message
                session.acknowledge_frame(&frame, AckOrNack::Ack).await?;
                break;
            }
            SessionEvent::ErrorFrame(frame) => {
                eprintln!("Error: {:?}", frame);
                break;
            }
            SessionEvent::Disconnected(reason) => {
                println!("Disconnected: {:?}", reason);
                break;
            }
            _ => continue,
        }
    }

    // Disconnect gracefully
    session.disconnect().await?;
    Ok(())
}
```

## Advanced Usage

### Connection Configuration

```rust
use stomp::session_builder::SessionBuilder;
use stomp::connection::{HeartBeat, Credentials};
use stomp::header::Header;

let mut session = SessionBuilder::new("localhost", 61613)
    // Add credentials
    .with(Credentials("username", "password"))
    // Set heartbeat (tx_ms, rx_ms)
    .with(HeartBeat(30000, 30000))
    // Add custom headers
    .with(Header::new("custom-header", "value"))
    .start()
    .await?;
```

### Transaction Support

```rust
// Begin a transaction
let mut transaction = session.begin_transaction();
transaction.begin().await?;

// Send messages within transaction
session.message("/queue/test", "msg1").send().await?;
session.message("/queue/test", "msg2").send().await?;

// Commit the transaction
transaction.commit().await?;
```

### Message Properties and Headers

```rust
use stomp::header::Header;

// Send message with custom headers
session
    .message("/queue/test", "Hello")
    .with(Header::new("priority", "high"))
    .with(Header::new("content-type", "text/plain"))
    .send()
    .await?;
```

### Stream-based Event Processing

```rust
use futures::StreamExt;

// Process events using Stream trait
let mut event_stream = session;
while let Some(event) = event_stream.next_event().await {
    match event {
        SessionEvent::Message { destination, frame, ack_mode } => {
            println!("Message from {}: {:?}", destination, frame.body);
            session.acknowledge_frame(&frame, AckOrNack::Ack).await?;
        }
        SessionEvent::Receipt { id, original, receipt } => {
            println!("Receipt {} received", id);
        }
        SessionEvent::ErrorFrame(frame) => {
            eprintln!("Server error: {:?}", frame);
        }
        SessionEvent::Disconnected(reason) => {
            println!("Connection lost: {:?}", reason);
            break;
        }
        _ => {}
    }
}
```

## Message Brokers Compatibility

This library has been tested with:

- **Apache ActiveMQ** 5.x and newer
- **RabbitMQ** with STOMP plugin
- **Apache Artemis**
- Other STOMP 1.2 compliant brokers

## Migration from 0.12.x

This is a major breaking release. See [CHANGELOG.md](CHANGELOG.md) for a detailed migration guide.

Key changes:
- All I/O operations are now `async` and require `.await`
- Updated to modern tokio 1.x and futures 0.3
- Rust 2021 edition and modern dependency versions
- Improved error handling and type safety

## Examples

See the [examples](examples/) directory for complete working examples:

- [send_and_subscribe](examples/send_and_subscribe/) - Basic usage with async/await

Run an example:
```bash
# Start a STOMP broker (e.g., ActiveMQ on localhost:61613)
cargo run --example send_and_subscribe
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Original stomp-rs implementation by Zack Slayton
- STOMP protocol specification: http://stomp.github.io/
- Built with modern Rust async ecosystem (tokio, futures, bytes, nom)
