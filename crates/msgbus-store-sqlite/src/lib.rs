use async_trait::async_trait;
use msgbus_core::{
    FetchQuery, HeadId, MessageId, MsgbusError, MsgbusStore, NewFifoMessage, NewMessage, NodeId,
    QueueName, Result, StoredFifoMessage, StoredMessage, TombstoneRange, Topic, now_ms,
};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;
use std::sync::Mutex;

pub struct SqliteStore {
    conn: Mutex<Connection>,
}

impl SqliteStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path).map_err(storage_err)?;
        configure(&conn)?;
        initialize(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory().map_err(storage_err)?;
        configure(&conn)?;
        initialize(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn connection(&self) -> Result<std::sync::MutexGuard<'_, Connection>> {
        self.conn
            .lock()
            .map_err(|err| MsgbusError::Storage(format!("sqlite connection lock poisoned: {err}")))
    }
}

#[async_trait]
impl MsgbusStore for SqliteStore {
    async fn append_message(&self, message: NewMessage) -> Result<StoredMessage> {
        let mut conn = self.connection()?;
        let tx = conn.transaction().map_err(storage_err)?;
        let topic = message.topic.as_str().to_string();
        let origin_key = message.origin.key();
        let current = tx
            .query_row(
                "SELECT head_id FROM heads WHERE topic = ?1 AND origin_key = ?2",
                params![topic, origin_key],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(storage_err)?
            .map(i64_to_u64)
            .transpose()?
            .unwrap_or(0);
        let next = current.saturating_add(1);
        tx.execute(
            "INSERT INTO heads(topic, origin_key, head_id)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(topic, origin_key) DO UPDATE SET head_id = excluded.head_id",
            params![topic, origin_key, u64_to_i64(next)?],
        )
        .map_err(storage_err)?;

        let stored = StoredMessage {
            id: message.id,
            topic: message.topic,
            origin: message.origin,
            head_id: HeadId(next),
            payload: message.payload,
            headers: message.headers,
            created_at_ms: now_ms(),
            deleted: false,
        };

        tx.execute(
            "INSERT INTO messages(
                topic, origin_key, origin_json, head_id, id, payload, headers_json, created_at_ms, deleted
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                stored.topic.as_str(),
                stored.origin.key(),
                encode_json(&stored.origin)?,
                u64_to_i64(stored.head_id.0)?,
                stored.id.as_str(),
                stored.payload,
                encode_json(&stored.headers)?,
                u64_to_i64(stored.created_at_ms)?,
                false,
            ],
        )
        .map_err(storage_err)?;
        tx.commit().map_err(storage_err)?;
        Ok(stored)
    }

    async fn fetch_messages(&self, query: FetchQuery) -> Result<Vec<StoredMessage>> {
        let conn = self.connection()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, topic, origin_json, head_id, payload, headers_json, created_at_ms, deleted
                 FROM messages
                 WHERE topic = ?1 AND origin_key = ?2 AND head_id >= ?3
                 ORDER BY head_id ASC
                 LIMIT ?4",
            )
            .map_err(storage_err)?;
        let topic = query.topic.as_str().to_string();
        let origin_key = query.origin.key();
        let rows = stmt
            .query_map(
                params![
                    topic,
                    origin_key,
                    u64_to_i64(query.from_head_id.0)?,
                    effective_limit(query.limit) as u32,
                ],
                row_to_message,
            )
            .map_err(storage_err)?;

        let mut messages = Vec::new();
        for row in rows {
            messages.push(row.map_err(storage_err)?);
        }
        Ok(messages)
    }

    async fn get_head(&self, topic: &Topic, origin: &NodeId) -> Result<Option<HeadId>> {
        let conn = self.connection()?;
        let head = conn
            .query_row(
                "SELECT head_id FROM heads WHERE topic = ?1 AND origin_key = ?2",
                params![topic.as_str(), origin.key()],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(storage_err)?;
        Ok(head.map(i64_to_u64).transpose()?.map(HeadId))
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

        let mut conn = self.connection()?;
        let tx = conn.transaction().map_err(storage_err)?;
        let changed = tx
            .execute(
                "UPDATE messages
                 SET deleted = TRUE
                 WHERE topic = ?1 AND origin_key = ?2 AND head_id >= ?3 AND head_id <= ?4 AND deleted = FALSE",
                params![
                    topic.as_str(),
                    origin.key(),
                    u64_to_i64(from.0)?,
                    u64_to_i64(to.0)?
                ],
            )
            .map_err(storage_err)? as u64;
        let tombstone = TombstoneRange {
            topic: topic.clone(),
            origin: origin.clone(),
            from_head_id: from,
            to_head_id: to,
            created_at_ms: now_ms(),
        };
        tx.execute(
            "INSERT INTO tombstones(topic, origin_key, from_head_id, to_head_id, created_at_ms, range_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                topic.as_str(),
                origin.key(),
                u64_to_i64(from.0)?,
                u64_to_i64(to.0)?,
                u64_to_i64(tombstone.created_at_ms)?,
                encode_json(&tombstone)?,
            ],
        )
        .map_err(storage_err)?;
        tx.commit().map_err(storage_err)?;
        Ok(changed)
    }

    async fn enqueue_fifo(&self, message: NewFifoMessage) -> Result<StoredFifoMessage> {
        let mut conn = self.connection()?;
        let tx = conn.transaction().map_err(storage_err)?;
        let queue = message.queue.as_str().to_string();
        let current = tx
            .query_row(
                "SELECT sequence FROM fifo_sequences WHERE queue = ?1",
                params![queue],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(storage_err)?
            .map(i64_to_u64)
            .transpose()?
            .unwrap_or(0);
        let sequence = current.saturating_add(1);
        tx.execute(
            "INSERT INTO fifo_sequences(queue, sequence)
             VALUES (?1, ?2)
             ON CONFLICT(queue) DO UPDATE SET sequence = excluded.sequence",
            params![queue, u64_to_i64(sequence)?],
        )
        .map_err(storage_err)?;

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

        tx.execute(
            "INSERT INTO fifo_messages(
                queue, sequence, id, topic, source_json, target_json, payload, headers_json, created_at_ms, attempts
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                stored.queue.as_str(),
                u64_to_i64(stored.sequence)?,
                stored.id.as_str(),
                stored.topic.as_str(),
                encode_json(&stored.source)?,
                encode_json(&stored.target)?,
                stored.payload,
                encode_json(&stored.headers)?,
                u64_to_i64(stored.created_at_ms)?,
                stored.attempts,
            ],
        )
        .map_err(storage_err)?;
        tx.commit().map_err(storage_err)?;
        Ok(stored)
    }

    async fn peek_fifo(&self, queue: &QueueName) -> Result<Option<StoredFifoMessage>> {
        let conn = self.connection()?;
        conn.query_row(
            "SELECT queue, sequence, id, topic, source_json, target_json, payload, headers_json, created_at_ms, attempts
             FROM fifo_messages
             WHERE queue = ?1
             ORDER BY sequence ASC
             LIMIT 1",
            params![queue.as_str()],
            row_to_fifo,
        )
        .optional()
        .map_err(storage_err)
    }

    async fn ack_fifo(&self, queue: &QueueName, message_id: &MessageId) -> Result<bool> {
        update_fifo_front(self, queue, message_id, FifoFrontAction::Ack)
    }

    async fn reject_fifo(&self, queue: &QueueName, message_id: &MessageId) -> Result<bool> {
        update_fifo_front(self, queue, message_id, FifoFrontAction::Reject)
    }
}

enum FifoFrontAction {
    Ack,
    Reject,
}

fn update_fifo_front(
    store: &SqliteStore,
    queue: &QueueName,
    message_id: &MessageId,
    action: FifoFrontAction,
) -> Result<bool> {
    let mut conn = store.connection()?;
    let tx = conn.transaction().map_err(storage_err)?;
    let front = tx
        .query_row(
            "SELECT queue, sequence, id, topic, source_json, target_json, payload, headers_json, created_at_ms, attempts
             FROM fifo_messages
             WHERE queue = ?1
             ORDER BY sequence ASC
             LIMIT 1",
            params![queue.as_str()],
            row_to_fifo,
        )
        .optional()
        .map_err(storage_err)?;

    let accepted = match front {
        Some(mut message) if message.id == *message_id => {
            match action {
                FifoFrontAction::Ack => {
                    tx.execute(
                        "DELETE FROM fifo_messages WHERE queue = ?1 AND sequence = ?2",
                        params![queue.as_str(), u64_to_i64(message.sequence)?],
                    )
                    .map_err(storage_err)?;
                }
                FifoFrontAction::Reject => {
                    message.attempts = message.attempts.saturating_add(1);
                    tx.execute(
                        "UPDATE fifo_messages
                         SET attempts = ?3
                         WHERE queue = ?1 AND sequence = ?2",
                        params![
                            queue.as_str(),
                            u64_to_i64(message.sequence)?,
                            message.attempts
                        ],
                    )
                    .map_err(storage_err)?;
                }
            }
            true
        }
        _ => false,
    };
    tx.commit().map_err(storage_err)?;
    Ok(accepted)
}

fn configure(conn: &Connection) -> Result<()> {
    conn.pragma_update(None, "journal_mode", "WAL")
        .map_err(storage_err)?;
    conn.pragma_update(None, "synchronous", "NORMAL")
        .map_err(storage_err)?;
    conn.pragma_update(None, "foreign_keys", "ON")
        .map_err(storage_err)?;
    Ok(())
}

fn initialize(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS heads (
            topic TEXT NOT NULL,
            origin_key TEXT NOT NULL,
            head_id INTEGER NOT NULL,
            PRIMARY KEY (topic, origin_key)
        );

        CREATE TABLE IF NOT EXISTS messages (
            topic TEXT NOT NULL,
            origin_key TEXT NOT NULL,
            origin_json TEXT NOT NULL,
            head_id INTEGER NOT NULL,
            id TEXT NOT NULL,
            payload BLOB NOT NULL,
            headers_json TEXT NOT NULL,
            created_at_ms INTEGER NOT NULL,
            deleted INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (topic, origin_key, head_id)
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_id ON messages(id);

        CREATE TABLE IF NOT EXISTS tombstones (
            topic TEXT NOT NULL,
            origin_key TEXT NOT NULL,
            from_head_id INTEGER NOT NULL,
            to_head_id INTEGER NOT NULL,
            created_at_ms INTEGER NOT NULL,
            range_json TEXT NOT NULL,
            PRIMARY KEY (topic, origin_key, from_head_id, to_head_id)
        );

        CREATE TABLE IF NOT EXISTS fifo_sequences (
            queue TEXT PRIMARY KEY,
            sequence INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS fifo_messages (
            queue TEXT NOT NULL,
            sequence INTEGER NOT NULL,
            id TEXT NOT NULL,
            topic TEXT NOT NULL,
            source_json TEXT NOT NULL,
            target_json TEXT NOT NULL,
            payload BLOB NOT NULL,
            headers_json TEXT NOT NULL,
            created_at_ms INTEGER NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (queue, sequence)
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_fifo_messages_id ON fifo_messages(id);
        ",
    )
    .map_err(storage_err)
}

fn row_to_message(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredMessage> {
    let id: String = row.get(0)?;
    let topic: String = row.get(1)?;
    let origin_json: String = row.get(2)?;
    let head_id = i64_to_u64(row.get::<_, i64>(3)?).map_err(sql_from_msgbus)?;
    let payload: Vec<u8> = row.get(4)?;
    let headers_json: String = row.get(5)?;
    let created_at_ms = i64_to_u64(row.get::<_, i64>(6)?).map_err(sql_from_msgbus)?;
    let deleted: bool = row.get(7)?;

    Ok(StoredMessage {
        id: MessageId::from_string(id).map_err(sql_from_msgbus)?,
        topic: Topic::new(topic).map_err(sql_from_msgbus)?,
        origin: decode_json(&origin_json).map_err(sql_from_msgbus)?,
        head_id: HeadId(head_id),
        payload,
        headers: decode_json(&headers_json).map_err(sql_from_msgbus)?,
        created_at_ms,
        deleted,
    })
}

fn row_to_fifo(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredFifoMessage> {
    let queue: String = row.get(0)?;
    let sequence = i64_to_u64(row.get::<_, i64>(1)?).map_err(sql_from_msgbus)?;
    let id: String = row.get(2)?;
    let topic: String = row.get(3)?;
    let source_json: String = row.get(4)?;
    let target_json: String = row.get(5)?;
    let payload: Vec<u8> = row.get(6)?;
    let headers_json: String = row.get(7)?;
    let created_at_ms = i64_to_u64(row.get::<_, i64>(8)?).map_err(sql_from_msgbus)?;
    let attempts: u32 = row.get(9)?;

    Ok(StoredFifoMessage {
        id: MessageId::from_string(id).map_err(sql_from_msgbus)?,
        queue: QueueName::new(queue).map_err(sql_from_msgbus)?,
        topic: Topic::new(topic).map_err(sql_from_msgbus)?,
        source: decode_json(&source_json).map_err(sql_from_msgbus)?,
        target: decode_json(&target_json).map_err(sql_from_msgbus)?,
        payload,
        headers: decode_json(&headers_json).map_err(sql_from_msgbus)?,
        created_at_ms,
        sequence,
        attempts,
    })
}

fn effective_limit(limit: u32) -> usize {
    if limit == 0 {
        100
    } else {
        limit.min(10_000) as usize
    }
}

fn encode_json<T: serde::Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value).map_err(|err| MsgbusError::Serialization(err.to_string()))
}

fn decode_json<T: serde::de::DeserializeOwned>(value: &str) -> Result<T> {
    serde_json::from_str(value).map_err(|err| MsgbusError::Serialization(err.to_string()))
}

fn u64_to_i64(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| MsgbusError::InvalidArgument(format!("{value} exceeds i64")))
}

fn i64_to_u64(value: i64) -> Result<u64> {
    u64::try_from(value)
        .map_err(|_| MsgbusError::Storage(format!("negative integer stored in sqlite: {value}")))
}

fn storage_err(err: impl std::fmt::Display) -> MsgbusError {
    MsgbusError::Storage(err.to_string())
}

fn sql_from_msgbus(err: MsgbusError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(err))
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
        let store = SqliteStore::open_in_memory().expect("open store");
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
    async fn tombstone_marks_messages_without_changing_head() {
        let store = SqliteStore::open_in_memory().expect("open store");
        let origin = node();
        let topic = topic();
        for payload in [b"one".to_vec(), b"two".to_vec(), b"three".to_vec()] {
            store
                .append_message(NewMessage::new(
                    topic.clone(),
                    origin.clone(),
                    payload,
                    BTreeMap::new(),
                ))
                .await
                .expect("append");
        }

        let deleted = store
            .tombstone_range(&topic, &origin, HeadId(2), HeadId(3))
            .await
            .expect("delete range");
        assert_eq!(deleted, 2);
        assert_eq!(
            store
                .get_head(&topic, &origin)
                .await
                .expect("head")
                .expect("head exists"),
            HeadId(3)
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
        assert!(!messages[0].deleted);
        assert!(messages[1].deleted);
        assert!(messages[2].deleted);
    }

    #[tokio::test]
    async fn fifo_ack_only_accepts_front_message() {
        let store = SqliteStore::open_in_memory().expect("open store");
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
