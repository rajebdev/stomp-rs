use crate::option_setter::OptionSetter;
use crate::connection::{HeartBeat, OwnedCredentials};
use crate::header::{HeaderList, Header};
use crate::session::Session;
use crate::header_list;

use std::io;

#[derive(Clone)]
pub struct SessionConfig {
    pub host: String,
    pub port: u16,
    pub credentials: Option<OwnedCredentials>,
    pub heartbeat: HeartBeat,
    pub headers: HeaderList,
}

pub struct SessionBuilder {
    pub config: SessionConfig
}

impl SessionBuilder {
    pub fn new(host: &str, port: u16) -> SessionBuilder {
        let config = SessionConfig {
            host: host.to_owned(),
            port,
            credentials: None,
            heartbeat: HeartBeat(0, 0),
            headers: header_list![
                "host" => host,
                "accept-version" => "1.2",
                "content-length" => "0"
            ],
        };
        SessionBuilder { config }
    }

    #[allow(dead_code)]
    pub async fn start(self) -> io::Result<Session> {
        Session::new(self.config).await
    }

    #[allow(dead_code)]
    pub fn with<'b, T>(self, option_setter: T) -> SessionBuilder
        where T: OptionSetter<SessionBuilder>
    {
        option_setter.set_option(self)
    }
    
    /// Add a custom header to the session
    #[allow(dead_code)]
    pub fn add_header(mut self, key: &str, value: &str) -> Self {
        self.config.headers.push(Header::new(key, value));
        self
    }

    /// Add a raw header (without encoding) to the session
    #[allow(dead_code)]
    pub fn add_raw_header<K: Into<String>, V: Into<String>>(mut self, key: K, value: V) -> Self {
        self.config.headers.push(Header::new_raw(key, value));
        self
    }

    /// Add multiple headers from a vector of tuples
    #[allow(dead_code)]
    pub fn add_headers(mut self, headers: Vec<(&str, &str)>) -> Self {
        for (key, value) in headers {
            self.config.headers.push(Header::new(key, value));
        }
        self
    }
}
