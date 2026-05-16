use msgbus_proto::msgbus::v1::{
    AckFifoRequest, DeleteRangeRequest, EnqueueFifoRequest, FetchRequest, GetHeadRequest,
    HealthRequest, ListHeadsRequest, PeekFifoRequest, PublishRequest, RejectFifoRequest,
    SubscribeRequest,
};
pub use msgbus_proto::msgbus::v1::{
    FifoMessage, GetHeadResponse, MessageEnvelope, NodeId, SubscribeResponse, TopicHead,
    msgbus_service_client::MsgbusServiceClient,
};
use std::collections::HashMap;
use tonic::transport::Channel;

pub type Result<T> = std::result::Result<T, SdkError>;

#[derive(Debug, thiserror::Error)]
pub enum SdkError {
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("rpc error: {0}")]
    Status(#[from] tonic::Status),
}

#[derive(Clone)]
pub struct MsgbusClient {
    client: MsgbusServiceClient<Channel>,
    default_origin: Option<NodeId>,
}

impl MsgbusClient {
    pub async fn connect(endpoint: impl AsRef<str>) -> Result<Self> {
        let client = MsgbusServiceClient::connect(endpoint.as_ref().to_string()).await?;
        Ok(Self {
            client,
            default_origin: None,
        })
    }

    pub fn with_default_origin(mut self, origin: NodeId) -> Self {
        self.default_origin = Some(origin);
        self
    }

    pub async fn health(&mut self) -> Result<String> {
        let response = self.client.health(HealthRequest {}).await?.into_inner();
        Ok(response.status)
    }

    pub async fn publish(
        &mut self,
        topic: impl Into<String>,
        payload: impl Into<Vec<u8>>,
    ) -> Result<MessageEnvelope> {
        self.publish_with_headers(topic, payload, HashMap::new())
            .await
    }

    pub async fn publish_with_headers(
        &mut self,
        topic: impl Into<String>,
        payload: impl Into<Vec<u8>>,
        headers: HashMap<String, String>,
    ) -> Result<MessageEnvelope> {
        let response = self
            .client
            .publish(PublishRequest {
                topic: topic.into(),
                origin: self.default_origin.clone(),
                payload: payload.into(),
                headers,
            })
            .await?
            .into_inner();
        Ok(response
            .message
            .expect("server returned empty publish response"))
    }

    pub async fn fetch(
        &mut self,
        topic: impl Into<String>,
        origin: Option<NodeId>,
        from_head_id: u64,
        limit: u32,
    ) -> Result<Vec<MessageEnvelope>> {
        let mut stream = self
            .client
            .fetch(FetchRequest {
                topic: topic.into(),
                origin: origin.or_else(|| self.default_origin.clone()),
                from_head_id,
                limit,
            })
            .await?
            .into_inner();
        let mut messages = Vec::new();
        while let Some(response) = stream.message().await? {
            if let Some(message) = response.message {
                messages.push(message);
            }
        }
        Ok(messages)
    }

    pub async fn subscribe(
        &mut self,
        topic: impl Into<String>,
        origin: Option<NodeId>,
        from_head_id: u64,
    ) -> Result<tonic::Streaming<SubscribeResponse>> {
        Ok(self
            .client
            .subscribe(SubscribeRequest {
                topic: topic.into(),
                origin: origin.or_else(|| self.default_origin.clone()),
                from_head_id,
                replay_limit: 100,
            })
            .await?
            .into_inner())
    }

    pub async fn get_head(
        &mut self,
        topic: impl Into<String>,
        origin: Option<NodeId>,
    ) -> Result<GetHeadResponse> {
        Ok(self
            .client
            .get_head(GetHeadRequest {
                topic: topic.into(),
                origin: origin.or_else(|| self.default_origin.clone()),
            })
            .await?
            .into_inner())
    }

    pub async fn list_heads(&mut self) -> Result<Vec<TopicHead>> {
        Ok(self
            .client
            .list_heads(ListHeadsRequest {})
            .await?
            .into_inner()
            .heads)
    }

    pub async fn delete_range(
        &mut self,
        topic: impl Into<String>,
        origin: Option<NodeId>,
        from_head_id: u64,
        to_head_id: u64,
    ) -> Result<u64> {
        Ok(self
            .client
            .delete_range(DeleteRangeRequest {
                topic: topic.into(),
                origin: origin.or_else(|| self.default_origin.clone()),
                from_head_id,
                to_head_id,
            })
            .await?
            .into_inner()
            .deleted_count)
    }

    pub async fn enqueue_fifo(
        &mut self,
        queue: impl Into<String>,
        topic: impl Into<String>,
        target: NodeId,
        payload: impl Into<Vec<u8>>,
    ) -> Result<FifoMessage> {
        let response = self
            .client
            .enqueue_fifo(EnqueueFifoRequest {
                queue: queue.into(),
                topic: topic.into(),
                source: self.default_origin.clone(),
                target: Some(target),
                payload: payload.into(),
                headers: HashMap::new(),
            })
            .await?
            .into_inner();
        Ok(response
            .message
            .expect("server returned empty fifo response"))
    }

    pub async fn peek_fifo(&mut self, queue: impl Into<String>) -> Result<FifoMessage> {
        Ok(self
            .client
            .peek_fifo(PeekFifoRequest {
                queue: queue.into(),
            })
            .await?
            .into_inner()
            .message
            .expect("server returned empty fifo response"))
    }

    pub async fn ack_fifo(
        &mut self,
        queue: impl Into<String>,
        message_id: impl Into<String>,
    ) -> Result<bool> {
        Ok(self
            .client
            .ack_fifo(AckFifoRequest {
                queue: queue.into(),
                message_id: message_id.into(),
            })
            .await?
            .into_inner()
            .accepted)
    }

    pub async fn reject_fifo(
        &mut self,
        queue: impl Into<String>,
        message_id: impl Into<String>,
    ) -> Result<bool> {
        Ok(self
            .client
            .reject_fifo(RejectFifoRequest {
                queue: queue.into(),
                message_id: message_id.into(),
            })
            .await?
            .into_inner()
            .accepted)
    }
}
