use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::time::Duration;

/// Main configuration structure that mirrors the config.yaml file
#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub service: ServiceConfig,
    pub broker: BrokerConfig,
    pub destinations: DestinationsConfig,
    pub consumers: ConsumersConfig,
    pub logging: LoggingConfig,
    pub shutdown: ShutdownConfig,
    #[serde(default = "RetryConfig::default")]
    pub retry: RetryConfig,
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
    /// Maximum number of retry attempts. -1 means infinite retries.
    pub max_attempts: i32,
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
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
    #[serde(default)]
    pub workers_per_topic: HashMap<String, u32>,
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

impl RetryConfig {
    /// Calculate the delay for the given retry attempt using exponential backoff
    pub fn calculate_delay(&self, attempt: u32) -> Duration {
        if attempt == 0 {
            return Duration::from_millis(self.initial_delay_ms);
        }

        let delay_ms =
            (self.initial_delay_ms as f64) * self.backoff_multiplier.powi(attempt as i32);
        let capped_delay_ms = delay_ms.min(self.max_delay_ms as f64) as u64;

        Duration::from_millis(capped_delay_ms)
    }

    /// Check if we should retry based on the current attempt number
    pub fn should_retry(&self, attempt: u32) -> bool {
        if self.max_attempts < 0 {
            true // Infinite retries when negative
        } else {
            attempt < (self.max_attempts as u32)
        }
    }

    /// Get the next retry attempt number (0-indexed)
    pub fn next_attempt(&self, current_attempt: u32) -> u32 {
        current_attempt + 1
    }

    /// Check if infinite retries are enabled
    pub fn is_infinite_retries(&self) -> bool {
        self.max_attempts < 0
    }

    /// Create a RetryConfig with finite retry attempts
    pub fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts as i32;
        self
    }

    /// Create a RetryConfig with infinite retry attempts
    pub fn with_infinite_retries(mut self) -> Self {
        self.max_attempts = -1;
        self
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: -1, // Infinite retries by default
            initial_delay_ms: 1000,
            max_delay_ms: 30000,
            backoff_multiplier: 2.0,
        }
    }
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
        self.broker
            .credentials
            .as_ref()
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
            self.broker.heartbeat.client_receive_secs * 1000,
        )
    }

    /// Get number of workers for a specific queue (default: 1)
    pub fn get_queue_workers(&self, queue_name: &str) -> u32 {
        self.consumers
            .workers_per_queue
            .get(queue_name)
            .copied()
            .unwrap_or(1)
    }

    /// Get number of workers for a specific topic (default: 1)
    pub fn get_topic_workers(&self, topic_name: &str) -> u32 {
        self.consumers
            .workers_per_topic
            .get(topic_name)
            .copied()
            .unwrap_or(1)
    }

    /// Get all configured queues with their worker counts
    pub fn get_all_queue_workers(&self) -> Vec<(String, u32)> {
        self.destinations
            .queues
            .keys()
            .map(|name| (name.clone(), self.get_queue_workers(name)))
            .collect()
    }

    /// Get all configured topics with their worker counts
    pub fn get_all_topic_workers(&self) -> Vec<(String, u32)> {
        self.destinations
            .topics
            .keys()
            .map(|name| (name.clone(), self.get_topic_workers(name)))
            .collect()
    }

    /// Get all configured queue names
    pub fn get_all_queue_names(&self) -> Vec<String> {
        self.destinations.queues.keys().cloned().collect()
    }

    /// Get all configured topic names
    pub fn get_all_topic_names(&self) -> Vec<String> {
        self.destinations.topics.keys().cloned().collect()
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

logging:
  level: "info"
  output: "stdout"

shutdown:
  timeout_secs: 30
  grace_period_secs: 5

retry:
  max_attempts: 3
  initial_delay_ms: 500
  max_delay_ms: 5000
  backoff_multiplier: 2.0
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

        // Test retry configuration
        assert_eq!(config.retry.max_attempts, 3);
        assert_eq!(config.retry.initial_delay_ms, 500);
        assert_eq!(config.retry.max_delay_ms, 5000);
        assert_eq!(config.retry.backoff_multiplier, 2.0);

        // Test retry delay calculations
        let delay_0 = config.retry.calculate_delay(0);
        assert_eq!(delay_0.as_millis(), 500);

        let delay_1 = config.retry.calculate_delay(1);
        assert_eq!(delay_1.as_millis(), 1000); // 500 * 2^1

        let delay_2 = config.retry.calculate_delay(2);
        assert_eq!(delay_2.as_millis(), 2000); // 500 * 2^2

        // Test should_retry logic
        assert!(config.retry.should_retry(0));
        assert!(config.retry.should_retry(2));
        assert!(!config.retry.should_retry(3)); // max_attempts is 3, so attempt 3 should not retry
    }

    #[test]
    fn test_retry_config_default() {
        let retry_config = RetryConfig::default();
        assert_eq!(retry_config.max_attempts, -1); // Default is now infinite retries (-1)
        assert_eq!(retry_config.initial_delay_ms, 1000);
        assert_eq!(retry_config.max_delay_ms, 30000);
        assert_eq!(retry_config.backoff_multiplier, 2.0);

        // Test delay capping
        let large_delay = retry_config.calculate_delay(10); // Should be capped at max_delay_ms
        assert_eq!(large_delay.as_millis(), 30000);

        // Test infinite retry logic
        assert!(retry_config.should_retry(0));
        assert!(retry_config.should_retry(100));
        assert!(retry_config.should_retry(1000000)); // Should always retry with infinite retries
        assert!(retry_config.is_infinite_retries()); // Should return true for -1
    }
}
