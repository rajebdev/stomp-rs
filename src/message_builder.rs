use crate::session::{Session, ReceiptRequest, OutstandingReceipt};
use crate::frame::Frame;
use crate::option_setter::OptionSetter;
use crate::header::{Header, HeaderList};

pub struct MessageBuilder<'a> {
    pub session: &'a mut Session,
    pub frame: Frame,
    pub receipt_request: Option<ReceiptRequest>
}

impl<'a> MessageBuilder<'a> {
    pub fn new(session: &'a mut Session, frame: Frame) -> Self {
        MessageBuilder {
            session: session,
            frame: frame,
            receipt_request: None
        }
    }

    #[allow(dead_code)]
    pub async fn send(self) -> std::io::Result<()> {
        if self.receipt_request.is_some() {
            let request = self.receipt_request.unwrap();
            self.session.state.outstanding_receipts.insert(
                request.id,
                OutstandingReceipt::new(
                    self.frame.clone()
                )
            );
        }
        self.session.send_frame(self.frame).await
    }

    #[allow(dead_code)]
    pub fn with<T>(self, option_setter: T) -> MessageBuilder<'a>
        where T: OptionSetter<MessageBuilder<'a>>
    {
        option_setter.set_option(self)
    }

    // Header support methods
    
    /// Add a custom header to the message
    #[allow(dead_code)]
    pub fn add_header(mut self, key: &str, value: &str) -> Self {
        self.frame.headers.push(Header::new(key, value));
        self
    }

    /// Add a raw header (without encoding) to the message
    #[allow(dead_code)]
    pub fn add_raw_header<K: Into<String>, V: Into<String>>(mut self, key: K, value: V) -> Self {
        self.frame.headers.push(Header::new_raw(key, value));
        self
    }

    /// Add multiple headers from a vector of tuples
    #[allow(dead_code)]
    pub fn add_headers(mut self, headers: Vec<(&str, &str)>) -> Self {
        for (key, value) in headers {
            self.frame.headers.push(Header::new(key, value));
        }
        self
    }

    /// Add headers from a HeaderList
    #[allow(dead_code)]
    pub fn add_header_list(mut self, mut header_list: HeaderList) -> Self {
        self.frame.headers.concat(&mut header_list);
        self
    }

    /// Set content-type header
    #[allow(dead_code)]
    pub fn with_content_type(self, content_type: &str) -> Self {
        self.add_header("content-type", content_type)
    }

    /// Set destination header (useful for SEND frames)
    #[allow(dead_code)]
    pub fn with_destination(self, destination: &str) -> Self {
        self.add_header("destination", destination)
    }

    /// Set transaction header
    #[allow(dead_code)]
    pub fn with_transaction(self, transaction_id: &str) -> Self {
        self.add_header("transaction", transaction_id)
    }

    /// Set receipt header
    #[allow(dead_code)]
    pub fn with_receipt(self, receipt_id: &str) -> Self {
        self.add_header("receipt", receipt_id)
    }

    /// Set persistent header
    #[allow(dead_code)]
    pub fn with_persistent(self, persistent: bool) -> Self {
        self.add_header("persistent", if persistent { "true" } else { "false" })
    }

    /// Set priority header
    #[allow(dead_code)]
    pub fn with_priority(self, priority: u8) -> Self {
        self.add_header("priority", &priority.to_string())
    }

    /// Set expires header (timestamp in milliseconds)
    #[allow(dead_code)]
    pub fn with_expires(self, expires_ms: u64) -> Self {
        self.add_header("expires", &expires_ms.to_string())
    }

    /// Set custom application headers with a prefix
    #[allow(dead_code)]
    pub fn with_app_header(self, key: &str, value: &str) -> Self {
        let app_key = format!("app-{}", key);
        self.add_header(&app_key, value)
    }

    // Header query methods

    /// Check if a header exists
    #[allow(dead_code)]
    pub fn has_header(&self, key: &str) -> bool {
        self.frame.headers.get_header(key).is_some()
    }

    /// Get a header value by key
    #[allow(dead_code)]
    pub fn get_header(&self, key: &str) -> Option<&str> {
        self.frame.headers.get_header(key).map(|h| h.get_value())
    }

    /// Get all headers as a reference to HeaderList
    #[allow(dead_code)]
    pub fn get_headers(&self) -> &HeaderList {
        &self.frame.headers
    }

    /// Get a mutable reference to all headers
    #[allow(dead_code)]
    pub fn get_headers_mut(&mut self) -> &mut HeaderList {
        &mut self.frame.headers
    }

    // Header removal methods

    /// Remove a header by key
    #[allow(dead_code)]
    pub fn remove_header(mut self, key: &str) -> Self {
        self.frame.headers.retain(|header| header.get_key() != key);
        self
    }

    /// Remove multiple headers by their keys
    #[allow(dead_code)]
    pub fn remove_headers(mut self, keys: &[&str]) -> Self {
        for key in keys {
            self.frame.headers.retain(|header| header.get_key() != *key);
        }
        self
    }

    /// Clear all headers
    #[allow(dead_code)]
    pub fn clear_headers(mut self) -> Self {
        self.frame.headers = HeaderList::new();
        self
    }
}

#[cfg(test)]
mod tests {
    use crate::frame::{Frame, Command};
    use crate::header::{HeaderList, Header};

    // Since Session has private fields, we'll test the header functionality
    // by testing the methods that don't require session operations
    
    // Helper to create a simple test frame
    fn create_test_frame() -> Frame {
        Frame {
            command: Command::Send,
            headers: HeaderList::new(),
            body: vec![],
        }
    }

    // Mock tests for header methods (these test the logic without session dependency)
    #[test]
    fn test_frame_header_manipulation() {
        let mut frame = create_test_frame();
        
        // Test adding headers directly to frame
        frame.headers.push(Header::new("test-key", "test-value"));
        assert!(frame.headers.get_header("test-key").is_some());
        assert_eq!(frame.headers.get_header("test-key").unwrap().get_value(), "test-value");
        
        // Test adding multiple headers
        frame.headers.push(Header::new("key1", "value1"));
        frame.headers.push(Header::new("key2", "value2"));
        
        assert!(frame.headers.get_header("key1").is_some());
        assert!(frame.headers.get_header("key2").is_some());
        
        // Test header removal 
        frame.headers.retain(|header| header.get_key() != "key1");
        assert!(frame.headers.get_header("key1").is_none());
        assert!(frame.headers.get_header("key2").is_some());
    }

    #[test] 
    fn test_header_encoding() {
        let header = Header::new("test:key", "test\nvalue");
        
        // Test that headers are properly encoded
        assert!(header.get_key().contains("\\c")); // colon should be encoded
        assert!(header.get_value().contains("\\n")); // newline should be encoded
    }

    #[test]
    fn test_raw_header() {
        let header = Header::new_raw("raw-key", "raw-value");
        
        assert_eq!(header.get_key(), "raw-key");
        assert_eq!(header.get_value(), "raw-value");
    }

    #[test]
    fn test_header_list_operations() {
        let mut header_list = HeaderList::new();
        
        // Test adding headers
        header_list.push(Header::new("content-type", "application/json"));
        header_list.push(Header::new("destination", "/queue/test"));
        header_list.push(Header::new("priority", "5"));
        
        // Test retrieval
        assert!(header_list.get_header("content-type").is_some());
        assert_eq!(header_list.get_header("content-type").unwrap().get_value(), "application/json");
        
        assert!(header_list.get_header("destination").is_some());
        assert_eq!(header_list.get_header("destination").unwrap().get_value(), "/queue/test");
        
        // Test filtering
        header_list.retain(|header| header.get_key() != "priority");
        assert!(header_list.get_header("priority").is_none());
        assert!(header_list.get_header("content-type").is_some());
    }

    #[test]
    fn test_header_concat() {
        let mut header_list1 = HeaderList::new();
        header_list1.push(Header::new("key1", "value1"));
        header_list1.push(Header::new("key2", "value2"));
        
        let mut header_list2 = HeaderList::new();
        header_list2.push(Header::new("key3", "value3"));
        header_list2.push(Header::new("key4", "value4"));
        
        // Test concatenation
        header_list1.concat(&mut header_list2);
        
        assert!(header_list1.get_header("key1").is_some());
        assert!(header_list1.get_header("key2").is_some());
        assert!(header_list1.get_header("key3").is_some());
        assert!(header_list1.get_header("key4").is_some());
    }
}
