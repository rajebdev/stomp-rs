use env_logger;
use stomp::session_builder::SessionBuilder;
use stomp::connection::{HeartBeat, Credentials};
use stomp::header::Header;
use stomp::subscription::AckOrNack;
use stomp::option_setter::OptionSetter;
use futures::StreamExt;

// Option setters for the session builder
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    println!("Setting up STOMP client.");
    
    // Create session with configuration
    let mut session = SessionBuilder::new("10.0.7.127", 31333)
        .with(WithHeader(Header::new("custom-client-id", "hmspna4")))
        .with(WithHeartBeat(HeartBeat(5_000, 2_000)))
        .with(WithCredentials(Credentials("sullivan", "m1k4d0")))
        .start()
        .await?;
        
    println!("Session established, sending messages.");
    
    let destination = "/queue/modern_major_general_0";
    
    // Send messages
    session.message(destination, "Animal").send().await?;
    session.message(destination, "Vegetable").send().await?;
    session.message(destination, "Mineral").send().await?;
    
    println!("Messages sent, subscribing to destination: {}", destination);
    
    // Subscribe to the destination
    session.subscription(destination)
        .start()
        .await?;
    
    println!("Subscription established, listening for messages...");
    
    // Process events
    while let Some(event) = session.next().await {
        match event {
            stomp::session::SessionEvent::Connected => {
                println!("Session connected!");
            }
            stomp::session::SessionEvent::Message { destination, frame, ack_mode } => {
                let body = std::str::from_utf8(&frame.body).unwrap_or("<invalid utf8>");
                println!("Received message from '{}': '{}'", destination, body);
                
                // Acknowledge the message if needed
                if let stomp::subscription::AckMode::ClientIndividual = ack_mode {
                    session.acknowledge_frame(&frame, AckOrNack::Ack).await?;
                }
            }
            stomp::session::SessionEvent::ErrorFrame(frame) => {
                println!("Error frame received: {:?}", frame);
                break;
            }
            stomp::session::SessionEvent::Receipt { id, original: _, receipt: _ } => {
                println!("Receipt received for id: {}", id);
            }
            stomp::session::SessionEvent::Disconnected(reason) => {
                println!("Session disconnected: {:?}", reason);
                break;
            }
            _ => {
                println!("Other event: {:?}", event);
            }
        }
    }
    
    Ok(())
}
