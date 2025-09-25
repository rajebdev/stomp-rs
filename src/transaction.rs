use crate::frame::{Frame, ToFrameBody};
use crate::message_builder::MessageBuilder;
use crate::header::Header;
use crate::session::Session;

pub struct Transaction<'tx> {
    pub id: String,
    pub session: &'tx mut Session,
}

impl<'tx> Transaction<'tx> {
    pub fn new(session: &'tx mut Session)
               -> Transaction<'tx> {
        Transaction {
            id: format!("tx/{}", session.generate_transaction_id()),
            session: session,
        }
    }

    pub fn message<'builder, T: ToFrameBody>(&'builder mut self,
                                             destination: &str,
                                             body_convertible: T)
                                             -> MessageBuilder<'builder> {
        let mut send_frame = Frame::send(destination, body_convertible.to_frame_body());
        send_frame.headers.push(Header::new("transaction", self.id.as_ref()));
        MessageBuilder::new(self.session, send_frame)
    }

    // TODO: See if it's feasible to do this via command_sender

    pub async fn begin(&mut self) -> std::io::Result<()> {
        let begin_frame = Frame::begin(&self.id.to_string());
        self.session.send_frame(begin_frame).await
    }

    pub async fn commit(self) -> std::io::Result<()> {
        let commit_frame = Frame::commit(&self.id.to_string());
        self.session.send_frame(commit_frame).await
    }

    pub async fn abort(self) -> std::io::Result<()> {
        let abort_frame = Frame::abort(&self.id.to_string());
        self.session.send_frame(abort_frame).await
    }
}
