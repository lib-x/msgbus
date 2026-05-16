use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use msgbus_core::{
    FetchQuery, HeadId, MessageId, MsgbusError, MsgbusStore, NewFifoMessage, NewMessage, NodeId,
    QueueName, Result, StoredFifoMessage, StoredMessage, TombstoneRange, Topic, now_ms,
};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use std::path::Path;
use std::sync::Arc;

const MESSAGES: TableDefinition<&str, &[u8]> = TableDefinition::new("messages");
const HEADS: TableDefinition<&str, u64> = TableDefinition::new("heads");
const FIFO_MESSAGES: TableDefinition<&str, &[u8]> = TableDefinition::new("fifo_messages");
const FIFO_SEQUENCES: TableDefinition<&str, u64> = TableDefinition::new("fifo_sequences");
const TOMBSTONES: TableDefinition<&str, &[u8]> = TableDefinition::new("tombstones");

#[derive(Clone)]
pub struct RedbStore {
    db: Arc<Database>,
}

impl RedbStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = Database::create(path).map_err(storage_err)?;
        let store = Self { db: Arc::new(db) };
        store.initialize()?;
        Ok(store)
    }

    fn initialize(&self) -> Result<()> {
        let txn = self.db.begin_write().map_err(storage_err)?;
        {
            txn.open_table(MESSAGES).map_err(storage_err)?;
            txn.open_table(HEADS).map_err(storage_err)?;
            txn.open_table(FIFO_MESSAGES).map_err(storage_err)?;
            txn.open_table(FIFO_SEQUENCES).map_err(storage_err)?;
            txn.open_table(TOMBSTONES).map_err(storage_err)?;
        }
        txn.commit().map_err(storage_err)
    }
}

#[async_trait]
impl MsgbusStore for RedbStore {
    async fn append_message(&self, message: NewMessage) -> Result<StoredMessage> {
        let txn = self.db.begin_write().map_err(storage_err)?;
        let head_key = head_key(&message.topic, &message.origin);
        let next_head = {
            let mut heads = txn.open_table(HEADS).map_err(storage_err)?;
            let current = heads
                .get(head_key.as_str())
                .map_err(storage_err)?
                .map(|value| value.value())
                .unwrap_or(0);
            let next = current.saturating_add(1);
            heads.insert(head_key.as_str(), next).map_err(storage_err)?;
            HeadId(next)
        };

        let stored = StoredMessage {
            id: message.id,
            topic: message.topic,
            origin: message.origin,
            head_id: next_head,
            payload: message.payload,
            headers: message.headers,
            created_at_ms: now_ms(),
            deleted: false,
        };

        let key = message_key(&stored.topic, &stored.origin, stored.head_id);
        let encoded = encode(&stored)?;
        {
            let mut messages = txn.open_table(MESSAGES).map_err(storage_err)?;
            messages
                .insert(key.as_str(), encoded.as_slice())
                .map_err(storage_err)?;
        }
        txn.commit().map_err(storage_err)?;
        Ok(stored)
    }

    async fn fetch_messages(&self, query: FetchQuery) -> Result<Vec<StoredMessage>> {
        let txn = self.db.begin_read().map_err(storage_err)?;
        let messages = txn.open_table(MESSAGES).map_err(storage_err)?;
        let prefix = message_prefix(&query.topic, &query.origin);
        let start = message_key(&query.topic, &query.origin, query.from_head_id);
        let mut out = Vec::new();

        for item in messages.range(start.as_str()..).map_err(storage_err)? {
            let (key, value) = item.map_err(storage_err)?;
            let key = key.value();
            if !key.starts_with(&prefix) {
                break;
            }
            let message: StoredMessage = decode(value.value())?;
            out.push(message);
            if out.len() >= effective_limit(query.limit) {
                break;
            }
        }

        Ok(out)
    }

    async fn get_head(&self, topic: &Topic, origin: &NodeId) -> Result<Option<HeadId>> {
        let txn = self.db.begin_read().map_err(storage_err)?;
        let heads = txn.open_table(HEADS).map_err(storage_err)?;
        let key = head_key(topic, origin);
        let value = heads.get(key.as_str()).map_err(storage_err)?;
        Ok(value.map(|value| HeadId(value.value())))
    }

    async fn tombstone_range(
        &self,
        topic: &Topic,
        origin: &NodeId,
        from: HeadId,
        to: HeadId,
    ) -> Result<u64> {
        if from.0 > to.0 {
            return Err(MsgbusError::InvalidArgument(
                "from_head_id must be <= to_head_id".to_string(),
            ));
        }

        let txn = self.db.begin_write().map_err(storage_err)?;
        let prefix = message_prefix(topic, origin);
        let start = message_key(topic, origin, from);
        let mut changed = 0_u64;
        {
            let mut messages = txn.open_table(MESSAGES).map_err(storage_err)?;
            let mut updates = Vec::new();
            for item in messages.range(start.as_str()..).map_err(storage_err)? {
                let (key, value) = item.map_err(storage_err)?;
                let key = key.value().to_string();
                if !key.starts_with(&prefix) {
                    break;
                }
                let mut message: StoredMessage = decode(value.value())?;
                if message.head_id.0 > to.0 {
                    break;
                }
                if !message.deleted {
                    message.deleted = true;
                    updates.push((key, encode(&message)?));
                }
            }

            for (key, value) in updates {
                messages
                    .insert(key.as_str(), value.as_slice())
                    .map_err(storage_err)?;
                changed = changed.saturating_add(1);
            }
        }
        {
            let tombstone = TombstoneRange {
                topic: topic.clone(),
                origin: origin.clone(),
                from_head_id: from,
                to_head_id: to,
                created_at_ms: now_ms(),
            };
            let mut tombstones = txn.open_table(TOMBSTONES).map_err(storage_err)?;
            let key = tombstone_key(topic, origin, from, to);
            let value = encode(&tombstone)?;
            tombstones
                .insert(key.as_str(), value.as_slice())
                .map_err(storage_err)?;
        }
        txn.commit().map_err(storage_err)?;
        Ok(changed)
    }

    async fn enqueue_fifo(&self, message: NewFifoMessage) -> Result<StoredFifoMessage> {
        let txn = self.db.begin_write().map_err(storage_err)?;
        let sequence = {
            let mut sequences = txn.open_table(FIFO_SEQUENCES).map_err(storage_err)?;
            let sequence_key = fifo_sequence_key(&message.queue);
            let current = sequences
                .get(sequence_key.as_str())
                .map_err(storage_err)?
                .map(|value| value.value())
                .unwrap_or(0);
            let next = current.saturating_add(1);
            sequences
                .insert(sequence_key.as_str(), next)
                .map_err(storage_err)?;
            next
        };

        let stored = StoredFifoMessage {
            id: message.id,
            queue: message.queue,
            topic: message.topic,
            source: message.source,
            target: message.target,
            payload: message.payload,
            headers: message.headers,
            created_at_ms: now_ms(),
            sequence,
            attempts: 0,
        };
        let key = fifo_message_key(&stored.queue, stored.sequence);
        let value = encode(&stored)?;
        {
            let mut fifo = txn.open_table(FIFO_MESSAGES).map_err(storage_err)?;
            fifo.insert(key.as_str(), value.as_slice())
                .map_err(storage_err)?;
        }
        txn.commit().map_err(storage_err)?;
        Ok(stored)
    }

    async fn peek_fifo(&self, queue: &QueueName) -> Result<Option<StoredFifoMessage>> {
        let txn = self.db.begin_read().map_err(storage_err)?;
        let fifo = txn.open_table(FIFO_MESSAGES).map_err(storage_err)?;
        let prefix = fifo_prefix(queue);
        for item in fifo.range(prefix.as_str()..).map_err(storage_err)? {
            let (key, value) = item.map_err(storage_err)?;
            if !key.value().starts_with(&prefix) {
                break;
            }
            return decode(value.value()).map(Some);
        }
        Ok(None)
    }

    async fn ack_fifo(&self, queue: &QueueName, message_id: &MessageId) -> Result<bool> {
        update_fifo_front(&self.db, queue, message_id, FifoFrontAction::Ack)
    }

    async fn reject_fifo(&self, queue: &QueueName, message_id: &MessageId) -> Result<bool> {
        update_fifo_front(&self.db, queue, message_id, FifoFrontAction::Reject)
    }
}

enum FifoFrontAction {
    Ack,
    Reject,
}

fn update_fifo_front(
    db: &Database,
    queue: &QueueName,
    message_id: &MessageId,
    action: FifoFrontAction,
) -> Result<bool> {
    let txn = db.begin_write().map_err(storage_err)?;
    let accepted = {
        let mut fifo = txn.open_table(FIFO_MESSAGES).map_err(storage_err)?;
        let prefix = fifo_prefix(queue);
        let mut front: Option<(String, StoredFifoMessage)> = None;
        for item in fifo.range(prefix.as_str()..).map_err(storage_err)? {
            let (key, value) = item.map_err(storage_err)?;
            if !key.value().starts_with(&prefix) {
                break;
            }
            front = Some((key.value().to_string(), decode(value.value())?));
            break;
        }

        match front {
            Some((key, mut message)) if message.id == *message_id => {
                match action {
                    FifoFrontAction::Ack => {
                        fifo.remove(key.as_str()).map_err(storage_err)?;
                    }
                    FifoFrontAction::Reject => {
                        message.attempts = message.attempts.saturating_add(1);
                        let value = encode(&message)?;
                        fifo.insert(key.as_str(), value.as_slice())
                            .map_err(storage_err)?;
                    }
                }
                true
            }
            _ => false,
        }
    };
    txn.commit().map_err(storage_err)?;
    Ok(accepted)
}

fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|err| MsgbusError::Serialization(err.to_string()))
}

fn decode<T: serde::de::DeserializeOwned>(value: &[u8]) -> Result<T> {
    serde_json::from_slice(value).map_err(|err| MsgbusError::Serialization(err.to_string()))
}

fn storage_err(err: impl std::fmt::Display) -> MsgbusError {
    MsgbusError::Storage(err.to_string())
}

fn effective_limit(limit: u32) -> usize {
    if limit == 0 {
        100
    } else {
        limit.min(10_000) as usize
    }
}

fn enc(value: &str) -> String {
    URL_SAFE_NO_PAD.encode(value.as_bytes())
}

fn head_key(topic: &Topic, origin: &NodeId) -> String {
    format!("head/{}/{}", enc(topic.as_str()), enc(&origin.key()))
}

fn message_prefix(topic: &Topic, origin: &NodeId) -> String {
    format!("msg/{}/{}/", enc(topic.as_str()), enc(&origin.key()))
}

fn message_key(topic: &Topic, origin: &NodeId, head_id: HeadId) -> String {
    format!("{}{:020}", message_prefix(topic, origin), head_id.0)
}

fn tombstone_key(topic: &Topic, origin: &NodeId, from: HeadId, to: HeadId) -> String {
    format!(
        "tombstone/{}/{}/{:020}-{:020}",
        enc(topic.as_str()),
        enc(&origin.key()),
        from.0,
        to.0
    )
}

fn fifo_sequence_key(queue: &QueueName) -> String {
    format!("fifo-seq/{}", enc(queue.as_str()))
}

fn fifo_prefix(queue: &QueueName) -> String {
    format!("fifo/{}/", enc(queue.as_str()))
}

fn fifo_message_key(queue: &QueueName, sequence: u64) -> String {
    format!("{}{:020}", fifo_prefix(queue), sequence)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn node() -> NodeId {
        NodeId::new("tenant", "bl", "device").expect("valid node")
    }

    fn topic() -> Topic {
        Topic::new("events").expect("valid topic")
    }

    #[tokio::test]
    async fn appends_and_fetches_messages_in_head_order() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = RedbStore::open(dir.path().join("msgbus.redb")).expect("open store");
        let origin = node();
        let topic = topic();

        let first = store
            .append_message(NewMessage::new(
                topic.clone(),
                origin.clone(),
                b"one".to_vec(),
                BTreeMap::new(),
            ))
            .await
            .expect("append first");
        let second = store
            .append_message(NewMessage::new(
                topic.clone(),
                origin.clone(),
                b"two".to_vec(),
                BTreeMap::new(),
            ))
            .await
            .expect("append second");

        assert_eq!(first.head_id, HeadId(1));
        assert_eq!(second.head_id, HeadId(2));
        assert_eq!(
            store
                .get_head(&topic, &origin)
                .await
                .expect("head")
                .expect("head exists"),
            HeadId(2)
        );

        let messages = store
            .fetch_messages(FetchQuery {
                topic,
                origin,
                from_head_id: HeadId(1),
                limit: 10,
            })
            .await
            .expect("fetch");
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].payload, b"one");
        assert_eq!(messages[1].payload, b"two");
    }

    #[tokio::test]
    async fn fifo_ack_only_accepts_front_message() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = RedbStore::open(dir.path().join("msgbus.redb")).expect("open store");
        let source = node();
        let target = NodeId::new("tenant", "bl", "target").expect("valid target");
        let queue = QueueName::new("tenant/bl/target").expect("valid queue");
        let topic = topic();

        let first = store
            .enqueue_fifo(NewFifoMessage::new(
                queue.clone(),
                topic.clone(),
                source.clone(),
                target.clone(),
                b"one".to_vec(),
                BTreeMap::new(),
            ))
            .await
            .expect("enqueue first");
        let second = store
            .enqueue_fifo(NewFifoMessage::new(
                queue.clone(),
                topic,
                source,
                target,
                b"two".to_vec(),
                BTreeMap::new(),
            ))
            .await
            .expect("enqueue second");

        assert!(
            !store
                .ack_fifo(&queue, &second.id)
                .await
                .expect("ack second")
        );
        assert_eq!(
            store
                .peek_fifo(&queue)
                .await
                .expect("peek")
                .expect("front")
                .id,
            first.id
        );
        assert!(store.ack_fifo(&queue, &first.id).await.expect("ack first"));
        assert_eq!(
            store
                .peek_fifo(&queue)
                .await
                .expect("peek")
                .expect("front")
                .id,
            second.id
        );
    }
}
