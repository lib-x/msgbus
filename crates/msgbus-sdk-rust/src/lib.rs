//! Rust SDK for applications that talk to a local `msgbusd` daemon.
//!
//! The SDK is intentionally thin: it owns the gRPC client, applies the optional
//! default origin, and exposes convenience helpers for common msgbus workflows.
//! Durable storage, ordering, and peer synchronization stay inside `msgbusd`.

use msgbus_proto::msgbus::v1::{
    AckFifoRequest, DeleteRangeRequest, EnqueueFifoRequest, FetchRequest, GetHeadRequest,
    HealthRequest, ListHeadsRequest, ListPeerSyncStatesRequest, PeekFifoRequest, PublishRequest,
    ReadyRequest, RejectFifoRequest, SubscribeRequest,
};
pub use msgbus_proto::msgbus::v1::{
    FifoMessage, GetHeadResponse, MessageEnvelope, NodeId, PeerSyncState, ReadyResponse,
    SubscribeResponse, TopicHead, msgbus_service_client::MsgbusServiceClient,
};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use tonic::transport::Channel;
use tonic::{Code, Streaming};

/// Result type returned by the Rust SDK.
pub type Result<T> = std::result::Result<T, SdkError>;

/// Errors surfaced by the SDK.
#[derive(Debug, thiserror::Error)]
pub enum SdkError {
    /// The client could not connect to the daemon.
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    /// The daemon returned a gRPC status error.
    #[error("rpc error: {0}")]
    Status(#[from] tonic::Status),
    /// The daemon response violated the protobuf contract expected by the SDK.
    #[error("protocol error: {0}")]
    Protocol(String),
}

/// Client for the `msgbusd` gRPC API.
///
/// A `MsgbusClient` should normally connect to the local daemon running on the
/// same device or application host. The daemon is responsible for persistence
/// and peer synchronization.
#[derive(Clone)]
pub struct MsgbusClient {
    client: MsgbusServiceClient<Channel>,
    default_origin: Option<NodeId>,
}

/// Options for creating a subscription stream.
#[derive(Clone, Debug)]
pub struct SubscribeOptions {
    /// Origin node to read from. If omitted, the client's default origin is used.
    pub origin: Option<NodeId>,
    /// First `head_id` to deliver. Values below `1` are normalized by helpers.
    pub from_head_id: u64,
    /// Maximum number of stored messages replayed before live delivery starts.
    pub replay_limit: u32,
    /// Delay between reconnect attempts for resilient subscriptions.
    pub reconnect_delay: Duration,
}

impl Default for SubscribeOptions {
    fn default() -> Self {
        Self {
            origin: None,
            from_head_id: 1,
            replay_limit: 100,
            reconnect_delay: Duration::from_millis(250),
        }
    }
}

/// Subscription helper that reopens the gRPC stream after retryable failures.
///
/// The helper tracks the next expected `head_id`, so reconnects resume from the
/// first message that has not yet been returned to the caller.
pub struct ResilientSubscription {
    client: MsgbusServiceClient<Channel>,
    topic: String,
    origin: Option<NodeId>,
    next_head_id: u64,
    replay_limit: u32,
    reconnect_delay: Duration,
    stream: Option<Streaming<SubscribeResponse>>,
}

impl MsgbusClient {
    /// Connects to a `msgbusd` endpoint such as `http://127.0.0.1:50051`.
    pub async fn connect(endpoint: impl AsRef<str>) -> Result<Self> {
        let client = MsgbusServiceClient::connect(endpoint.as_ref().to_string()).await?;
        Ok(Self {
            client,
            default_origin: None,
        })
    }

    /// Sets the origin used when an API call does not provide one explicitly.
    ///
    /// For writes, `msgbusd` only accepts the local daemon origin. In normal
    /// deployments this value should match the daemon's configured node ID.
    pub fn with_default_origin(mut self, origin: NodeId) -> Self {
        self.default_origin = Some(origin);
        self
    }

    /// Returns daemon liveness status.
    ///
    /// This call verifies that the gRPC server is responding. Use [`Self::ready`] to
    /// check whether the daemon can also access its backing store.
    pub async fn health(&mut self) -> Result<String> {
        let response = self.client.health(HealthRequest {}).await?.into_inner();
        Ok(response.status)
    }

    /// Returns readiness status for storage-backed operations.
    ///
    /// A daemon can be live but not ready if its store cannot be read.
    pub async fn ready(&mut self) -> Result<ReadyResponse> {
        Ok(self.client.ready(ReadyRequest {}).await?.into_inner())
    }

    /// Publishes a message without headers.
    ///
    /// The daemon assigns the next `head_id` for the message's `topic + origin`.
    pub async fn publish(
        &mut self,
        topic: impl Into<String>,
        payload: impl Into<Vec<u8>>,
    ) -> Result<MessageEnvelope> {
        self.publish_with_headers(topic, payload, HashMap::new())
            .await
    }

    /// Publishes a message with application headers.
    ///
    /// Headers are opaque key/value metadata and are stored with the message.
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
        required(response.message, "server returned empty publish response")
    }

    /// Fetches stored messages for a `topic + origin` from `from_head_id`.
    ///
    /// A `limit` of zero lets the daemon choose its default page size.
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

    /// Opens a subscription stream with default replay settings.
    ///
    /// The stream first replays stored messages from `from_head_id`, then keeps
    /// delivering live messages published after the stream was opened.
    pub async fn subscribe(
        &mut self,
        topic: impl Into<String>,
        origin: Option<NodeId>,
        from_head_id: u64,
    ) -> Result<Streaming<SubscribeResponse>> {
        self.subscribe_with_options(
            topic,
            SubscribeOptions {
                origin,
                from_head_id,
                ..SubscribeOptions::default()
            },
        )
        .await
    }

    /// Opens a subscription stream with explicit replay and origin options.
    pub async fn subscribe_with_options(
        &mut self,
        topic: impl Into<String>,
        options: SubscribeOptions,
    ) -> Result<Streaming<SubscribeResponse>> {
        Ok(self
            .client
            .subscribe(SubscribeRequest {
                topic: topic.into(),
                origin: options.origin.or_else(|| self.default_origin.clone()),
                from_head_id: options.from_head_id,
                replay_limit: options.replay_limit,
            })
            .await?
            .into_inner())
    }

    /// Creates a reconnecting subscription with default replay settings.
    pub fn subscribe_reconnecting(
        &self,
        topic: impl Into<String>,
        origin: Option<NodeId>,
        from_head_id: u64,
    ) -> ResilientSubscription {
        self.subscribe_reconnecting_with_options(
            topic,
            SubscribeOptions {
                origin,
                from_head_id,
                ..SubscribeOptions::default()
            },
        )
    }

    /// Creates a reconnecting subscription with explicit options.
    ///
    /// Call [`ResilientSubscription::next_message`] to receive messages one at a
    /// time. Retryable stream failures reopen the subscription after
    /// `reconnect_delay`.
    pub fn subscribe_reconnecting_with_options(
        &self,
        topic: impl Into<String>,
        options: SubscribeOptions,
    ) -> ResilientSubscription {
        ResilientSubscription {
            client: self.client.clone(),
            topic: topic.into(),
            origin: options.origin.or_else(|| self.default_origin.clone()),
            next_head_id: options.from_head_id.max(1),
            replay_limit: options.replay_limit,
            reconnect_delay: options.reconnect_delay,
            stream: None,
        }
    }

    /// Returns the current head for a `topic + origin`.
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

    /// Lists all topic heads known to the local daemon.
    pub async fn list_heads(&mut self) -> Result<Vec<TopicHead>> {
        Ok(self
            .client
            .list_heads(ListHeadsRequest {})
            .await?
            .into_inner()
            .heads)
    }

    /// Lists persisted peer sync state for lag and failure inspection.
    pub async fn list_peer_sync_states(&mut self) -> Result<Vec<PeerSyncState>> {
        Ok(self
            .client
            .list_peer_sync_states(ListPeerSyncStatesRequest {})
            .await?
            .into_inner()
            .states)
    }

    /// Marks a range of messages as deleted.
    ///
    /// Deletions are represented as tombstones, preserving `head_id` ordering and
    /// allowing delete markers to replicate to peers.
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

    /// Enqueues a FIFO message without headers.
    ///
    /// FIFO queues require consumers to ack or reject the front message first.
    pub async fn enqueue_fifo(
        &mut self,
        queue: impl Into<String>,
        topic: impl Into<String>,
        target: NodeId,
        payload: impl Into<Vec<u8>>,
    ) -> Result<FifoMessage> {
        self.enqueue_fifo_with_headers(queue, topic, target, payload, HashMap::new())
            .await
    }

    /// Enqueues a FIFO message with application headers.
    pub async fn enqueue_fifo_with_headers(
        &mut self,
        queue: impl Into<String>,
        topic: impl Into<String>,
        target: NodeId,
        payload: impl Into<Vec<u8>>,
        headers: HashMap<String, String>,
    ) -> Result<FifoMessage> {
        let response = self
            .client
            .enqueue_fifo(EnqueueFifoRequest {
                queue: queue.into(),
                topic: topic.into(),
                source: self.default_origin.clone(),
                target: Some(target),
                payload: payload.into(),
                headers,
            })
            .await?
            .into_inner();
        required(response.message, "server returned empty fifo response")
    }

    /// Returns the current front message of a FIFO queue without removing it.
    pub async fn peek_fifo(&mut self, queue: impl Into<String>) -> Result<FifoMessage> {
        let response = self
            .client
            .peek_fifo(PeekFifoRequest {
                queue: queue.into(),
            })
            .await?
            .into_inner();
        required(response.message, "server returned empty fifo response")
    }

    /// Acknowledges the current front FIFO message and removes it from the queue.
    ///
    /// Returns `false` when `message_id` is not the current front message.
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

    /// Rejects the current front FIFO message and increments its attempt count.
    ///
    /// Returns `false` when `message_id` is not the current front message.
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

impl ResilientSubscription {
    /// Receives the next message, reconnecting on retryable stream failures.
    ///
    /// This method waits until a message is available or a non-retryable error
    /// occurs. It advances the resume point only after returning a message.
    pub async fn next_message(&mut self) -> Result<MessageEnvelope> {
        loop {
            if self.stream.is_none() {
                match self.open_stream().await {
                    Ok(()) => {}
                    Err(SdkError::Status(status)) if retryable_status(status.code()) => {
                        sleep(self.reconnect_delay).await;
                        continue;
                    }
                    Err(err) => return Err(err),
                }
            }

            let stream = self
                .stream
                .as_mut()
                .ok_or_else(|| SdkError::Protocol("subscription stream was not opened".into()))?;
            match stream.message().await {
                Ok(Some(response)) => {
                    let message =
                        required(response.message, "server returned empty subscribe response")?;
                    self.next_head_id = message.head_id.saturating_add(1);
                    return Ok(message);
                }
                Ok(None) => {
                    self.stream = None;
                    sleep(self.reconnect_delay).await;
                }
                Err(status) if retryable_status(status.code()) => {
                    self.stream = None;
                    sleep(self.reconnect_delay).await;
                }
                Err(status) => return Err(SdkError::Status(status)),
            }
        }
    }

    async fn open_stream(&mut self) -> Result<()> {
        let stream = self
            .client
            .subscribe(SubscribeRequest {
                topic: self.topic.clone(),
                origin: self.origin.clone(),
                from_head_id: self.next_head_id,
                replay_limit: self.replay_limit,
            })
            .await?
            .into_inner();
        self.stream = Some(stream);
        Ok(())
    }
}

fn required<T>(value: Option<T>, message: &str) -> Result<T> {
    value.ok_or_else(|| SdkError::Protocol(message.to_string()))
}

fn retryable_status(code: Code) -> bool {
    matches!(
        code,
        Code::Unavailable | Code::DeadlineExceeded | Code::Aborted | Code::Cancelled
    )
}
