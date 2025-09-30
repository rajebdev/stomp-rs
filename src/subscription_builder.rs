use crate::session::{Session, ReceiptRequest, OutstandingReceipt};
use crate::subscription::{Subscription, AckMode};
use crate::frame::Frame;
use crate::header::{Header, HeaderList};
use crate::option_setter::OptionSetter;

pub struct SubscriptionBuilder<'a> {
    pub session: &'a mut Session,
    pub destination: String,
    pub ack_mode: AckMode,
    pub headers: HeaderList,
    pub receipt_request: Option<ReceiptRequest>
}

impl<'a> SubscriptionBuilder<'a> {
    pub fn new(session: &'a mut Session, destination: String) -> Self {
        SubscriptionBuilder {
            session,
            destination,
            ack_mode: AckMode::Auto,
            headers: HeaderList::new(),
            receipt_request: None
        }
    }

    #[allow(dead_code)]
    pub async fn start(mut self) -> Result<String, std::io::Error> {
        let next_id = self.session.generate_subscription_id();
        let subscription = Subscription::new(
            next_id,
            &self.destination,
            self.ack_mode,
            self.headers.clone()
        );
        let mut subscribe_frame = Frame::subscribe(
            &subscription.id,
            &self.destination,
            self.ack_mode
        );

        subscribe_frame.headers.concat(&mut self.headers);

        self.session.send_frame(subscribe_frame.clone()).await?;

        log::debug!(
            "Registering callback for subscription id '{}' from builder",
            subscription.id
        );
        let id_to_return = subscription.id.to_string();
        self.session.state.subscriptions.insert(subscription.id.to_string(), subscription);
        
        if let Some(request) = self.receipt_request {
            self.session.state.outstanding_receipts.insert(
                request.id,
                OutstandingReceipt::new(subscribe_frame.clone())
            );
        }
        Ok(id_to_return)
    }

    #[allow(dead_code)]
    pub fn with<T>(self, option_setter: T) -> SubscriptionBuilder<'a>
        where T: OptionSetter<SubscriptionBuilder<'a>>
    {
        option_setter.set_option(self)
    }

    /// Add a custom header to the subscription
    #[allow(dead_code)]
    pub fn add_header(mut self, key: &str, value: &str) -> Self {
        self.headers.push(Header::new(key, value));
        self
    }

    /// Add a raw header (without encoding) to the subscription
    #[allow(dead_code)]
    pub fn add_raw_header<K: Into<String>, V: Into<String>>(mut self, key: K, value: V) -> Self {
        self.headers.push(Header::new_raw(key, value));
        self
    }

    /// Add multiple headers from a vector of tuples
    #[allow(dead_code)]
    pub fn add_headers(mut self, headers: Vec<(&str, &str)>) -> Self {
        for (key, value) in headers {
            self.headers.push(Header::new(key, value));
        }
        self
    }
}
