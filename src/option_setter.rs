use crate::message_builder::MessageBuilder;
use crate::session_builder::SessionBuilder;
use crate::subscription_builder::SubscriptionBuilder;
use crate::header::{Header, SuppressedHeader, ContentType, HeaderList};
use crate::connection::{HeartBeat, Credentials, OwnedCredentials};
use crate::subscription::AckMode;
use crate::session::{ReceiptRequest, GenerateReceipt};

pub trait OptionSetter<T> {
    fn set_option(self, target: T) -> T;
}

impl <'a> OptionSetter<MessageBuilder<'a>> for Header {
    fn set_option(self, mut builder: MessageBuilder<'a>) -> MessageBuilder<'a> {
        builder.frame.headers.push(self);
        builder
    }
}

impl <'a, 'b> OptionSetter<MessageBuilder<'b>> for SuppressedHeader<'a> {
    fn set_option(self, mut builder: MessageBuilder<'b>) -> MessageBuilder<'b> {
        let SuppressedHeader(key) = self;
        builder.frame.headers.retain(|header| (*header).get_key() != key);
        builder
    }
}

impl <'a, 'b> OptionSetter<MessageBuilder<'b>> for ContentType<'a> {
    fn set_option(self, mut builder: MessageBuilder<'b>) -> MessageBuilder<'b> {
        let ContentType(content_type) = self;
        builder.frame.headers.push(Header::new("content-type", content_type));
        builder
    }
}

impl OptionSetter<SessionBuilder> for Header {
    fn set_option(self, mut builder: SessionBuilder) -> SessionBuilder {
        builder.config.headers.push(self);
        builder
    }
}

impl OptionSetter<SessionBuilder> for HeartBeat {
    fn set_option(self, mut builder: SessionBuilder) -> SessionBuilder {
        builder.config.heartbeat = self;
        builder
    }
}

impl<'b> OptionSetter<SessionBuilder> for Credentials<'b> {
    fn set_option(self, mut builder: SessionBuilder) -> SessionBuilder {
        builder.config.credentials = Some(OwnedCredentials::from(self));
        builder
    }
}

impl<'b> OptionSetter<SessionBuilder> for SuppressedHeader<'b> {
    fn set_option(self, mut builder: SessionBuilder) -> SessionBuilder {
        let SuppressedHeader(key) = self;
        builder.config.headers.retain(|header| (*header).get_key() != key);
        builder
    }
}

impl <'a> OptionSetter<SubscriptionBuilder<'a>> for Header {
    fn set_option(self, mut builder: SubscriptionBuilder<'a>) -> SubscriptionBuilder<'a> {
        builder.headers.push(self);
        builder
    }
}

impl <'a, 'b> OptionSetter<SubscriptionBuilder<'b>> for SuppressedHeader<'a> {
    fn set_option(self, mut builder: SubscriptionBuilder<'b>) -> SubscriptionBuilder<'b> {
        let SuppressedHeader(key) = self;
        builder.headers.retain(|header| (*header).get_key() != key);
        builder
    }
}

impl <'a> OptionSetter<SubscriptionBuilder<'a>> for AckMode  {
    fn set_option(self, mut builder: SubscriptionBuilder<'a>) -> SubscriptionBuilder<'a> {
        builder.ack_mode = self;
        builder
    }
}

impl <'a> OptionSetter<MessageBuilder<'a>> for GenerateReceipt {
    fn set_option(self, mut builder: MessageBuilder<'a>) -> MessageBuilder<'a> {
        let next_id = builder.session.generate_receipt_id();
        let receipt_id = format!("message/{}", next_id);
        builder.receipt_request = Some(ReceiptRequest::new(receipt_id.clone()));
        builder.frame.headers.push(Header::new("receipt", receipt_id.as_ref()));
        builder
    }
}

impl <'a> OptionSetter<SubscriptionBuilder<'a>> for GenerateReceipt {
    fn set_option(self, mut builder: SubscriptionBuilder<'a>) -> SubscriptionBuilder<'a> {
        let next_id = builder.session.generate_receipt_id();
        let receipt_id = format!("message/{}", next_id);
        builder.receipt_request = Some(ReceiptRequest::new(receipt_id.clone()));
        builder.headers.push(Header::new("receipt", receipt_id.as_ref()));
        builder
    }
}

// Additional OptionSetter implementations for MessageBuilder convenience

/// Convenience wrapper for setting destination header
pub struct Destination<'a>(pub &'a str);

impl<'a, 'b> OptionSetter<MessageBuilder<'b>> for Destination<'a> {
    fn set_option(self, builder: MessageBuilder<'b>) -> MessageBuilder<'b> {
        builder.with_destination(self.0)
    }
}

/// Convenience wrapper for setting transaction header
pub struct Transaction<'a>(pub &'a str);

impl<'a, 'b> OptionSetter<MessageBuilder<'b>> for Transaction<'a> {
    fn set_option(self, builder: MessageBuilder<'b>) -> MessageBuilder<'b> {
        builder.with_transaction(self.0)
    }
}

/// Convenience wrapper for setting receipt header
pub struct ReceiptId<'a>(pub &'a str);

impl<'a, 'b> OptionSetter<MessageBuilder<'b>> for ReceiptId<'a> {
    fn set_option(self, builder: MessageBuilder<'b>) -> MessageBuilder<'b> {
        builder.with_receipt(self.0)
    }
}

/// Convenience wrapper for setting persistent header
pub struct Persistent(pub bool);

impl<'a> OptionSetter<MessageBuilder<'a>> for Persistent {
    fn set_option(self, builder: MessageBuilder<'a>) -> MessageBuilder<'a> {
        builder.with_persistent(self.0)
    }
}

/// Convenience wrapper for setting priority header
pub struct Priority(pub u8);

impl<'a> OptionSetter<MessageBuilder<'a>> for Priority {
    fn set_option(self, builder: MessageBuilder<'a>) -> MessageBuilder<'a> {
        builder.with_priority(self.0)
    }
}

/// Convenience wrapper for setting expires header
pub struct Expires(pub u64);

impl<'a> OptionSetter<MessageBuilder<'a>> for Expires {
    fn set_option(self, builder: MessageBuilder<'a>) -> MessageBuilder<'a> {
        builder.with_expires(self.0)
    }
}

/// Convenience wrapper for adding multiple headers at once
pub struct Headers(pub Vec<(&'static str, String)>);

impl<'a> OptionSetter<MessageBuilder<'a>> for Headers {
    fn set_option(self, mut builder: MessageBuilder<'a>) -> MessageBuilder<'a> {
        for (key, value) in self.0 {
            builder = builder.add_header(key, &value);
        }
        builder
    }
}

/// Convenience wrapper for adding a HeaderList
pub struct HeaderListOption(pub HeaderList);

impl<'a> OptionSetter<MessageBuilder<'a>> for HeaderListOption {
    fn set_option(self, builder: MessageBuilder<'a>) -> MessageBuilder<'a> {
        builder.add_header_list(self.0)
    }
}

/// Convenience wrapper for application-specific headers
pub struct AppHeader<'a> {
    pub key: &'a str,
    pub value: &'a str,
}

impl<'a, 'b> OptionSetter<MessageBuilder<'b>> for AppHeader<'a> {
    fn set_option(self, builder: MessageBuilder<'b>) -> MessageBuilder<'b> {
        builder.with_app_header(self.key, self.value)
    }
}
