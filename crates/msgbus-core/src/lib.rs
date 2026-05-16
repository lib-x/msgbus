use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

pub type Result<T> = std::result::Result<T, MsgbusError>;

#[derive(Debug, thiserror::Error)]
pub enum MsgbusError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("serialization error: {0}")]
    Serialization(String),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId {
    pub tenant_id: String,
    pub bl_name: String,
    pub device_id: String,
}

impl NodeId {
    pub fn new(
        tenant_id: impl Into<String>,
        bl_name: impl Into<String>,
        device_id: impl Into<String>,
    ) -> Result<Self> {
        let node = Self {
            tenant_id: tenant_id.into(),
            bl_name: bl_name.into(),
            device_id: device_id.into(),
        };
        node.validate()?;
        Ok(node)
    }

    pub fn is_empty(&self) -> bool {
        self.tenant_id.is_empty() && self.bl_name.is_empty() && self.device_id.is_empty()
    }

    pub fn key(&self) -> String {
        format!("{}/{}/{}", self.tenant_id, self.bl_name, self.device_id)
    }

    pub fn validate(&self) -> Result<()> {
        validate_part("tenant_id", &self.tenant_id)?;
        validate_part("bl_name", &self.bl_name)?;
        validate_part("device_id", &self.device_id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Topic(String);

impl Topic {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_part("topic", &value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

#[derive(
    Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct HeadId(pub u64);

impl HeadId {
    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MessageId(String);

impl MessageId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn from_string(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_part("message_id", &value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for MessageId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NewMessage {
    pub id: MessageId,
    pub topic: Topic,
    pub origin: NodeId,
    pub payload: Vec<u8>,
    pub headers: BTreeMap<String, String>,
}

impl NewMessage {
    pub fn new(
        topic: Topic,
        origin: NodeId,
        payload: Vec<u8>,
        headers: BTreeMap<String, String>,
    ) -> Self {
        Self {
            id: MessageId::new(),
            topic,
            origin,
            payload,
            headers,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredMessage {
    pub id: MessageId,
    pub topic: Topic,
    pub origin: NodeId,
    pub head_id: HeadId,
    pub payload: Vec<u8>,
    pub headers: BTreeMap<String, String>,
    pub created_at_ms: u64,
    pub deleted: bool,
}

#[derive(Clone, Debug)]
pub struct FetchQuery {
    pub topic: Topic,
    pub origin: NodeId,
    pub from_head_id: HeadId,
    pub limit: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TombstoneRange {
    pub topic: Topic,
    pub origin: NodeId,
    pub from_head_id: HeadId,
    pub to_head_id: HeadId,
    pub created_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct QueueName(String);

impl QueueName {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_part("queue", &value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NewFifoMessage {
    pub id: MessageId,
    pub queue: QueueName,
    pub topic: Topic,
    pub source: NodeId,
    pub target: NodeId,
    pub payload: Vec<u8>,
    pub headers: BTreeMap<String, String>,
}

impl NewFifoMessage {
    pub fn new(
        queue: QueueName,
        topic: Topic,
        source: NodeId,
        target: NodeId,
        payload: Vec<u8>,
        headers: BTreeMap<String, String>,
    ) -> Self {
        Self {
            id: MessageId::new(),
            queue,
            topic,
            source,
            target,
            payload,
            headers,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredFifoMessage {
    pub id: MessageId,
    pub queue: QueueName,
    pub topic: Topic,
    pub source: NodeId,
    pub target: NodeId,
    pub payload: Vec<u8>,
    pub headers: BTreeMap<String, String>,
    pub created_at_ms: u64,
    pub sequence: u64,
    pub attempts: u32,
}

#[async_trait]
pub trait MsgbusStore: Send + Sync + 'static {
    async fn append_message(&self, message: NewMessage) -> Result<StoredMessage>;

    async fn fetch_messages(&self, query: FetchQuery) -> Result<Vec<StoredMessage>>;

    async fn get_head(&self, topic: &Topic, origin: &NodeId) -> Result<Option<HeadId>>;

    async fn tombstone_range(
        &self,
        topic: &Topic,
        origin: &NodeId,
        from: HeadId,
        to: HeadId,
    ) -> Result<u64>;

    async fn enqueue_fifo(&self, message: NewFifoMessage) -> Result<StoredFifoMessage>;

    async fn peek_fifo(&self, queue: &QueueName) -> Result<Option<StoredFifoMessage>>;

    async fn ack_fifo(&self, queue: &QueueName, message_id: &MessageId) -> Result<bool>;

    async fn reject_fifo(&self, queue: &QueueName, message_id: &MessageId) -> Result<bool>;
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

fn validate_part(name: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(MsgbusError::InvalidArgument(format!("{name} is empty")));
    }
    if value.contains('\0') {
        return Err(MsgbusError::InvalidArgument(format!(
            "{name} contains NUL byte"
        )));
    }
    Ok(())
}
