use crate::backend::GenericSocketBackend;
use crate::codec::Message;
use crate::transport::AcceptStopHandle;
use crate::{
    BlockingSend, Endpoint, MultiPeerBackend, Socket, SocketBackend, SocketType, ZmqMessage,
    ZmqResult,
};
use async_trait::async_trait;
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::sync::Arc;

pub struct PushSocket {
    backend: Arc<GenericSocketBackend>,
    binds: HashMap<Endpoint, AcceptStopHandle>,
}

impl Drop for PushSocket {
    fn drop(&mut self) {
        self.backend.shutdown();
    }
}

#[async_trait]
impl Socket for PushSocket {
    fn new() -> Self {
        Self {
            backend: Arc::new(GenericSocketBackend::new(None, SocketType::PUSH)),
            binds: HashMap::new(),
        }
    }
    fn backend(&self) -> Arc<dyn MultiPeerBackend> {
        self.backend.clone()
    }

    fn binds(&mut self) -> &mut HashMap<Endpoint, AcceptStopHandle, RandomState> {
        &mut self.binds
    }
}

#[async_trait]
impl BlockingSend for PushSocket {
    async fn send(&mut self, message: ZmqMessage) -> ZmqResult<()> {
        self.backend
            .send_round_robin(Message::Message(message))
            .await?;
        Ok(())
    }
}