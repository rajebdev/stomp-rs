use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;

/// Main configuration structure that mirrors the config.yaml file
#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub service: ServiceConfig,
    pub broker: BrokerConfig,
    pub destinations: DestinationsConfig,
    pub consumers: ConsumersConfig,
    pub logging: LoggingConfig,
    pub shutdown: ShutdownConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServiceConfig {
    pub name: String,
    pub version: String,
    pub description: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct BrokerConfig {
    pub host: String,
    pub port: u16,
    pub credentials: Option<CredentialsConfig>,
    pub heartbeat: HeartbeatConfig,
    pub retry: RetryConfig,
    pub headers: HashMap<String, String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CredentialsConfig {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HeartbeatConfig {
    pub client_send_secs: u32,
    pub client_receive_secs: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub initial_delay_secs: u32,
    pub max_delay_secs: u32,
    pub backoff_multiplier: f64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DestinationsConfig {
    pub queues: HashMap<String, DestinationConfig>,
    pub topics: HashMap<String, DestinationConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DestinationConfig {
    pub path: String,
    pub headers: HashMap<String, String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ConsumersConfig {
    pub ack_mode: String,
    pub workers_per_queue: HashMap<String, u32>,
    pub retry: RetryConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct LoggingConfig {
    pub level: String,
    pub output: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ShutdownConfig {
    pub timeout_secs: u32,
    pub grace_period_secs: u32,
}

impl Config {
    /// Load configuration from a YAML file
    pub fn load(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path))?;
        
        let config: Config = serde_yaml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path))?;
        
        Ok(config)
    }
    
    /// Get the broker connection URL
    pub fn get_broker_url(&self) -> String {
        format!("{}:{}", self.broker.host, self.broker.port)
    }
    
    /// Get credentials if available
    pub fn get_credentials(&self) -> Option<(String, String)> {
        self.broker.credentials.as_ref()
            .map(|creds| (creds.username.clone(), creds.password.clone()))
    }
    
    /// Get a specific queue configuration
    pub fn get_queue_config(&self, name: &str) -> Option<&DestinationConfig> {
        self.destinations.queues.get(name)
    }
    
    /// Get a specific topic configuration
    pub fn get_topic_config(&self, name: &str) -> Option<&DestinationConfig> {
        self.destinations.topics.get(name)
    }
    
    /// Get heartbeat settings in milliseconds (converting from seconds)
    pub fn get_heartbeat_ms(&self) -> (u32, u32) {
        (
            self.broker.heartbeat.client_send_secs * 1000,
            self.broker.heartbeat.client_receive_secs * 1000
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_config_parsing() {
        let yaml_content = r#"
service:
  name: "test-service"
  version: "1.0.0"
  description: "Test service"

broker:
  host: "localhost"
  port: 61613
  credentials:
    username: "admin"
    password: "admin"
  heartbeat:
    client_send_secs: 30
    client_receive_secs: 30
  retry:
    max_attempts: 3
    initial_delay_secs: 1
    max_delay_secs: 60
    backoff_multiplier: 2.0
  headers: {}

destinations:
  queues:
    test:
      path: "/queue/test"
      headers: {}
  topics:
    notifications:
      path: "/topic/notifications"
      headers: {}

consumers:
  ack_mode: "client_individual"
  workers_per_queue:
    test: 1
  retry:
    max_attempts: 3
    initial_delay_secs: 1
    max_delay_secs: 30
    backoff_multiplier: 2.0

logging:
  level: "info"
  output: "stdout"

shutdown:
  timeout_secs: 30
  grace_period_secs: 5
        "#;
        
        let config: Config = serde_yaml::from_str(yaml_content).unwrap();
        assert_eq!(config.service.name, "test-service");
        assert_eq!(config.broker.host, "localhost");
        assert_eq!(config.broker.port, 61613);
        
        let (username, password) = config.get_credentials().unwrap();
        assert_eq!(username, "admin");
        assert_eq!(password, "admin");
        
        let (send_ms, recv_ms) = config.get_heartbeat_ms();
        assert_eq!(send_ms, 30000);
        assert_eq!(recv_ms, 30000);
    }
}