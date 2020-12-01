mod command;
mod error;
mod framed;
mod greeting;
pub(crate) mod mechanism;
mod zmq_codec;

pub(crate) use command::{ZmqCommand, ZmqCommandName};
pub(crate) use error::{CodecError, CodecResult};
pub(crate) use framed::{FrameableRead, FrameableWrite, FramedIo, ZmqFramedRead, ZmqFramedWrite};
pub(crate) use greeting::{ZmqGreeting, ZmtpVersion};
pub(crate) use zmq_codec::ZmqCodec;

use crate::message::ZmqMessage;
use crate::{ZmqError, ZmqResult};
use futures::task::Poll;
use futures::Sink;
use std::pin::Pin;

#[derive(Debug, Clone)]
pub enum Message {
    Greeting(ZmqGreeting),
    Command(ZmqCommand),
    Message(ZmqMessage),
    Multipart(Vec<ZmqMessage>),
}

pub(crate) trait TrySend {
    fn try_send(self: Pin<&mut Self>, item: Message) -> ZmqResult<()>;
}

impl TrySend for ZmqFramedWrite {
    fn try_send(mut self: Pin<&mut Self>, item: Message) -> ZmqResult<()> {
        let waker = futures::task::noop_waker();
        let mut cx = futures::task::Context::from_waker(&waker);
        match self.as_mut().poll_ready(&mut cx) {
            Poll::Ready(Ok(())) => {
                self.as_mut().start_send(item)?;
                let _ = self.as_mut().poll_flush(&mut cx); // ignore result just hope that it flush eventually
                Ok(())
            }
            Poll::Ready(Err(e)) => Err(e.into()),
            Poll::Pending => Err(ZmqError::BufferFull("Sink is full")),
        }
    }
}