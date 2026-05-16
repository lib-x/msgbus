use crate::{apply_control_message, core_message, core_topic_head};
use msgbus_core::{HeadId, MsgbusStore, ReplicateResult, StoredMessage};
use msgbus_proto::msgbus::v1::{
    FetchRequest, ListHeadsRequest, msgbus_service_client::MsgbusServiceClient,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tonic::Status;
use tracing::{debug, warn};

type SyncResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Debug)]
pub struct PeerSyncConfig {
    pub peers: Vec<String>,
    pub interval: Duration,
    pub batch_limit: u32,
}

impl PeerSyncConfig {
    pub fn disabled() -> Self {
        Self {
            peers: Vec::new(),
            interval: Duration::from_secs(1),
            batch_limit: 100,
        }
    }

    pub fn is_enabled(&self) -> bool {
        !self.peers.is_empty()
    }
}

pub fn spawn_peer_sync<S>(
    store: Arc<S>,
    published: broadcast::Sender<StoredMessage>,
    config: PeerSyncConfig,
) -> JoinHandle<()>
where
    S: MsgbusStore,
{
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(config.interval);
        loop {
            interval.tick().await;
            for peer in &config.peers {
                if let Err(err) =
                    sync_peer(store.clone(), published.clone(), peer, config.batch_limit).await
                {
                    warn!(peer, error = %err, "peer sync failed");
                }
            }
        }
    })
}

async fn sync_peer<S>(
    store: Arc<S>,
    published: broadcast::Sender<StoredMessage>,
    peer: &str,
    batch_limit: u32,
) -> SyncResult<()>
where
    S: MsgbusStore,
{
    let mut client = MsgbusServiceClient::connect(peer.to_string()).await?;
    let heads = client
        .list_heads(ListHeadsRequest {})
        .await?
        .into_inner()
        .heads;

    for head in heads {
        let head = core_topic_head(head).map_err(status_box)?;
        let mut local_head = store
            .get_head(&head.topic, &head.origin)
            .await?
            .unwrap_or(HeadId(0))
            .0;

        while local_head < head.head_id.0 {
            let before = local_head;
            let mut fetched = 0_u32;
            let mut stream = client
                .fetch(FetchRequest {
                    topic: head.topic.as_str().to_string(),
                    origin: Some(msgbus_proto::msgbus::v1::NodeId {
                        tenant_id: head.origin.tenant_id.clone(),
                        bl_name: head.origin.bl_name.clone(),
                        device_id: head.origin.device_id.clone(),
                    }),
                    from_head_id: local_head.saturating_add(1),
                    limit: batch_limit.max(1),
                })
                .await?
                .into_inner();

            while let Some(response) = stream.message().await? {
                let Some(message) = response.message else {
                    continue;
                };
                let message = core_message(message).map_err(status_box)?;
                if message.topic != head.topic || message.origin != head.origin {
                    return Err(format!(
                        "peer {peer} returned mismatched message for topic={} origin={}",
                        head.topic.as_str(),
                        head.origin.key()
                    )
                    .into());
                }

                match store.put_replicated_message(message.clone()).await? {
                    ReplicateResult::Inserted => {
                        apply_control_message(store.as_ref(), &message)
                            .await
                            .map_err(status_box)?;
                        let _ = published.send(message.clone());
                        debug!(
                            peer,
                            topic = message.topic.as_str(),
                            origin = message.origin.key(),
                            head_id = message.head_id.0,
                            "replicated message"
                        );
                    }
                    ReplicateResult::AlreadyPresent => {
                        apply_control_message(store.as_ref(), &message)
                            .await
                            .map_err(status_box)?;
                    }
                }
                local_head = local_head.max(message.head_id.0);
                fetched = fetched.saturating_add(1);
            }

            if fetched == 0 || local_head == before {
                break;
            }
        }
    }

    Ok(())
}

fn status_box(status: Status) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(status)
}
