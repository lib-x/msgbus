use futures_core::Stream;
use msgbus_core::{
    FetchQuery, HeadId, MessageId, MsgbusError, MsgbusStore, NewFifoMessage, NewMessage, NodeId,
    QueueName, StoredFifoMessage, StoredMessage, TombstoneRange, Topic, TopicHead, now_ms,
};
use msgbus_proto::msgbus::v1::{
    AckFifoRequest, AckFifoResponse, DeleteRangeRequest, DeleteRangeResponse, EnqueueFifoRequest,
    EnqueueFifoResponse, FetchRequest, FetchResponse, FifoMessage, GetHeadRequest, GetHeadResponse,
    HealthRequest, HealthResponse, ListHeadsRequest, ListHeadsResponse, MessageEnvelope,
    PeekFifoRequest, PeekFifoResponse, PublishRequest, PublishResponse, RejectFifoRequest,
    RejectFifoResponse, SubscribeRequest, SubscribeResponse,
    msgbus_service_server::{MsgbusService, MsgbusServiceServer},
};
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream};
use tonic::{Request, Response, Status};

pub mod sync;

const DEFAULT_REPLAY_LIMIT: u32 = 100;
pub(crate) const DELETE_TOPIC: &str = "$msgbus.delete";

#[derive(Clone)]
pub struct MsgbusGrpcService<S> {
    store: Arc<S>,
    node: NodeId,
    published: broadcast::Sender<StoredMessage>,
}

impl<S> MsgbusGrpcService<S>
where
    S: MsgbusStore,
{
    pub fn new(store: Arc<S>, node: NodeId) -> Self {
        let (published, _) = broadcast::channel(1024);
        Self {
            store,
            node,
            published,
        }
    }

    pub fn into_server(self) -> MsgbusServiceServer<Self> {
        MsgbusServiceServer::new(self)
    }

    pub fn published_sender(&self) -> broadcast::Sender<StoredMessage> {
        self.published.clone()
    }

    fn request_origin(
        &self,
        origin: Option<msgbus_proto::msgbus::v1::NodeId>,
    ) -> Result<NodeId, Status> {
        match origin {
            Some(origin) => {
                let node = core_node(origin)?;
                if node.is_empty() {
                    Ok(self.node.clone())
                } else {
                    Ok(node)
                }
            }
            None => Ok(self.node.clone()),
        }
    }
}

type RpcStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl<S> MsgbusService for MsgbusGrpcService<S>
where
    S: MsgbusStore,
{
    type FetchStream = RpcStream<FetchResponse>;
    type SubscribeStream = RpcStream<SubscribeResponse>;

    async fn publish(
        &self,
        request: Request<PublishRequest>,
    ) -> Result<Response<PublishResponse>, Status> {
        let request = request.into_inner();
        let topic = Topic::new(request.topic).map_err(status_from_error)?;
        let origin = self.request_origin(request.origin)?;
        let message = NewMessage::new(
            topic,
            origin,
            request.payload,
            request.headers.into_iter().collect(),
        );
        let stored = self
            .store
            .append_message(message)
            .await
            .map_err(status_from_error)?;
        let _ = self.published.send(stored.clone());
        Ok(Response::new(PublishResponse {
            message: Some(proto_message(stored)),
        }))
    }

    async fn fetch(
        &self,
        request: Request<FetchRequest>,
    ) -> Result<Response<Self::FetchStream>, Status> {
        let request = request.into_inner();
        let topic = Topic::new(request.topic).map_err(status_from_error)?;
        let origin = self.request_origin(request.origin)?;
        let messages = self
            .store
            .fetch_messages(FetchQuery {
                topic,
                origin,
                from_head_id: HeadId(request.from_head_id.max(1)),
                limit: request.limit,
            })
            .await
            .map_err(status_from_error)?;
        let stream = tokio_stream::iter(messages.into_iter().map(|message| {
            Ok(FetchResponse {
                message: Some(proto_message(message)),
            })
        }));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let request = request.into_inner();
        let topic = Topic::new(request.topic).map_err(status_from_error)?;
        let origin = self.request_origin(request.origin)?;
        let from_head_id = HeadId(request.from_head_id.max(1));
        let replay_limit = if request.replay_limit == 0 {
            DEFAULT_REPLAY_LIMIT
        } else {
            request.replay_limit
        };

        let replay = self
            .store
            .fetch_messages(FetchQuery {
                topic: topic.clone(),
                origin: origin.clone(),
                from_head_id,
                limit: replay_limit,
            })
            .await
            .map_err(status_from_error)?;

        let mut live = BroadcastStream::new(self.published.subscribe());
        let (tx, rx) = mpsc::channel(128);
        tokio::spawn(async move {
            let mut last_sent = from_head_id.0.saturating_sub(1);
            for message in replay {
                last_sent = last_sent.max(message.head_id.0);
                if tx
                    .send(Ok(SubscribeResponse {
                        message: Some(proto_message(message)),
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
            }

            while let Some(item) = live.next().await {
                match item {
                    Ok(message)
                        if message.topic == topic
                            && message.origin == origin
                            && message.head_id.0 > last_sent =>
                    {
                        last_sent = message.head_id.0;
                        if tx
                            .send(Ok(SubscribeResponse {
                                message: Some(proto_message(message)),
                            }))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    Ok(_) => {}
                    Err(err) => {
                        let status = Status::unavailable(format!("subscription lagged: {err}"));
                        let _ = tx.send(Err(status)).await;
                        return;
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn get_head(
        &self,
        request: Request<GetHeadRequest>,
    ) -> Result<Response<GetHeadResponse>, Status> {
        let request = request.into_inner();
        let topic = Topic::new(request.topic).map_err(status_from_error)?;
        let origin = self.request_origin(request.origin)?;
        let head = self
            .store
            .get_head(&topic, &origin)
            .await
            .map_err(status_from_error)?;
        Ok(Response::new(GetHeadResponse {
            head_id: head.map_or(0, |head| head.0),
            found: head.is_some(),
        }))
    }

    async fn list_heads(
        &self,
        _request: Request<ListHeadsRequest>,
    ) -> Result<Response<ListHeadsResponse>, Status> {
        let heads = self
            .store
            .list_heads()
            .await
            .map_err(status_from_error)?
            .into_iter()
            .map(proto_topic_head)
            .collect();
        Ok(Response::new(ListHeadsResponse { heads }))
    }

    async fn delete_range(
        &self,
        request: Request<DeleteRangeRequest>,
    ) -> Result<Response<DeleteRangeResponse>, Status> {
        let request = request.into_inner();
        let topic = Topic::new(request.topic).map_err(status_from_error)?;
        let origin = self.request_origin(request.origin)?;
        let from = HeadId(request.from_head_id);
        let to = HeadId(request.to_head_id);
        let deleted_count = self
            .store
            .tombstone_range(&topic, &origin, from, to)
            .await
            .map_err(status_from_error)?;
        let tombstone = TombstoneRange {
            topic,
            origin,
            from_head_id: from,
            to_head_id: to,
            created_at_ms: now_ms(),
        };
        let payload = serde_json::to_vec(&tombstone)
            .map_err(|err| Status::internal(format!("failed to encode tombstone: {err}")))?;
        let control = self
            .store
            .append_message(NewMessage::new(
                Topic::new(DELETE_TOPIC).map_err(status_from_error)?,
                self.node.clone(),
                payload,
                BTreeMap::new(),
            ))
            .await
            .map_err(status_from_error)?;
        let _ = self.published.send(control);
        Ok(Response::new(DeleteRangeResponse { deleted_count }))
    }

    async fn enqueue_fifo(
        &self,
        request: Request<EnqueueFifoRequest>,
    ) -> Result<Response<EnqueueFifoResponse>, Status> {
        let request = request.into_inner();
        let queue = QueueName::new(request.queue).map_err(status_from_error)?;
        let topic = Topic::new(request.topic).map_err(status_from_error)?;
        let source = self.request_origin(request.source)?;
        let target = request
            .target
            .map(core_node)
            .transpose()?
            .ok_or_else(|| Status::invalid_argument("target is required"))?;
        let stored = self
            .store
            .enqueue_fifo(NewFifoMessage::new(
                queue,
                topic,
                source,
                target,
                request.payload,
                request.headers.into_iter().collect(),
            ))
            .await
            .map_err(status_from_error)?;
        Ok(Response::new(EnqueueFifoResponse {
            message: Some(proto_fifo(stored)),
        }))
    }

    async fn peek_fifo(
        &self,
        request: Request<PeekFifoRequest>,
    ) -> Result<Response<PeekFifoResponse>, Status> {
        let queue = QueueName::new(request.into_inner().queue).map_err(status_from_error)?;
        let message = self
            .store
            .peek_fifo(&queue)
            .await
            .map_err(status_from_error)?
            .ok_or_else(|| Status::not_found("fifo queue is empty"))?;
        Ok(Response::new(PeekFifoResponse {
            message: Some(proto_fifo(message)),
        }))
    }

    async fn ack_fifo(
        &self,
        request: Request<AckFifoRequest>,
    ) -> Result<Response<AckFifoResponse>, Status> {
        let (queue, message_id) = fifo_ack_parts(request.into_inner())?;
        let accepted = self
            .store
            .ack_fifo(&queue, &message_id)
            .await
            .map_err(status_from_error)?;
        Ok(Response::new(AckFifoResponse { accepted }))
    }

    async fn reject_fifo(
        &self,
        request: Request<RejectFifoRequest>,
    ) -> Result<Response<RejectFifoResponse>, Status> {
        let (queue, message_id) = reject_fifo_parts(request.into_inner())?;
        let accepted = self
            .store
            .reject_fifo(&queue, &message_id)
            .await
            .map_err(status_from_error)?;
        Ok(Response::new(RejectFifoResponse { accepted }))
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            status: "SERVING".to_string(),
            node: Some(proto_node(self.node.clone())),
        }))
    }
}

fn fifo_ack_parts(request: AckFifoRequest) -> Result<(QueueName, MessageId), Status> {
    Ok((
        QueueName::new(request.queue).map_err(status_from_error)?,
        MessageId::from_string(request.message_id).map_err(status_from_error)?,
    ))
}

fn reject_fifo_parts(request: RejectFifoRequest) -> Result<(QueueName, MessageId), Status> {
    Ok((
        QueueName::new(request.queue).map_err(status_from_error)?,
        MessageId::from_string(request.message_id).map_err(status_from_error)?,
    ))
}

fn core_node(value: msgbus_proto::msgbus::v1::NodeId) -> Result<NodeId, Status> {
    if value.tenant_id.is_empty() && value.bl_name.is_empty() && value.device_id.is_empty() {
        return Ok(NodeId {
            tenant_id: String::new(),
            bl_name: String::new(),
            device_id: String::new(),
        });
    }
    NodeId::new(value.tenant_id, value.bl_name, value.device_id).map_err(status_from_error)
}

fn proto_node(value: NodeId) -> msgbus_proto::msgbus::v1::NodeId {
    msgbus_proto::msgbus::v1::NodeId {
        tenant_id: value.tenant_id,
        bl_name: value.bl_name,
        device_id: value.device_id,
    }
}

fn proto_message(value: StoredMessage) -> MessageEnvelope {
    MessageEnvelope {
        id: value.id.as_str().to_string(),
        topic: value.topic.into_string(),
        origin: Some(proto_node(value.origin)),
        head_id: value.head_id.0,
        payload: value.payload,
        headers: map_headers(value.headers),
        created_at_ms: value.created_at_ms,
        deleted: value.deleted,
    }
}

pub(crate) fn core_message(value: MessageEnvelope) -> Result<StoredMessage, Status> {
    let origin = value
        .origin
        .map(core_node)
        .transpose()?
        .ok_or_else(|| Status::invalid_argument("message origin is required"))?;
    if value.head_id == 0 {
        return Err(Status::invalid_argument(
            "message head_id must be greater than zero",
        ));
    }
    Ok(StoredMessage {
        id: MessageId::from_string(value.id).map_err(status_from_error)?,
        topic: Topic::new(value.topic).map_err(status_from_error)?,
        origin,
        head_id: HeadId(value.head_id),
        payload: value.payload,
        headers: value.headers.into_iter().collect(),
        created_at_ms: value.created_at_ms,
        deleted: value.deleted,
    })
}

fn proto_topic_head(value: TopicHead) -> msgbus_proto::msgbus::v1::TopicHead {
    msgbus_proto::msgbus::v1::TopicHead {
        topic: value.topic.into_string(),
        origin: Some(proto_node(value.origin)),
        head_id: value.head_id.0,
    }
}

pub(crate) fn core_topic_head(
    value: msgbus_proto::msgbus::v1::TopicHead,
) -> Result<TopicHead, Status> {
    let origin = value
        .origin
        .map(core_node)
        .transpose()?
        .ok_or_else(|| Status::invalid_argument("topic head origin is required"))?;
    Ok(TopicHead {
        topic: Topic::new(value.topic).map_err(status_from_error)?,
        origin,
        head_id: HeadId(value.head_id),
    })
}

pub(crate) async fn apply_control_message<S>(
    store: &S,
    message: &StoredMessage,
) -> Result<(), Status>
where
    S: MsgbusStore,
{
    if message.topic.as_str() != DELETE_TOPIC || message.deleted {
        return Ok(());
    }
    let tombstone: TombstoneRange = serde_json::from_slice(&message.payload)
        .map_err(|err| Status::invalid_argument(format!("invalid tombstone payload: {err}")))?;
    store
        .tombstone_range(
            &tombstone.topic,
            &tombstone.origin,
            tombstone.from_head_id,
            tombstone.to_head_id,
        )
        .await
        .map_err(status_from_error)?;
    Ok(())
}

fn proto_fifo(value: StoredFifoMessage) -> FifoMessage {
    FifoMessage {
        id: value.id.as_str().to_string(),
        queue: value.queue.as_str().to_string(),
        topic: value.topic.into_string(),
        source: Some(proto_node(value.source)),
        target: Some(proto_node(value.target)),
        payload: value.payload,
        headers: map_headers(value.headers),
        created_at_ms: value.created_at_ms,
        sequence: value.sequence,
        attempts: value.attempts,
    }
}

fn map_headers(value: BTreeMap<String, String>) -> std::collections::HashMap<String, String> {
    value.into_iter().collect()
}

fn status_from_error(error: MsgbusError) -> Status {
    match error {
        MsgbusError::InvalidArgument(message) => Status::invalid_argument(message),
        MsgbusError::NotFound(message) => Status::not_found(message),
        MsgbusError::Conflict(message) => Status::failed_precondition(message),
        MsgbusError::Storage(message) | MsgbusError::Serialization(message) => {
            Status::internal(message)
        }
    }
}
