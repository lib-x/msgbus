use crate::{apply_control_message, core_message, core_node, core_topic_head};
use msgbus_core::{
    HeadId, MsgbusError, MsgbusStore, NodeId, PeerSyncState, ReplicateResult, StoredMessage,
    TopicHead, now_ms,
};
use msgbus_proto::msgbus::v1::{
    FetchRequest, HealthRequest, ListHeadsRequest, msgbus_service_client::MsgbusServiceClient,
};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;
use tonic::Status;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tracing::{debug, warn};

pub type SyncResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Debug)]
pub struct PeerSpec {
    pub endpoint: String,
    pub expected_node: Option<NodeId>,
}

impl PeerSpec {
    pub fn parse(value: impl Into<String>) -> Result<Self, MsgbusError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(MsgbusError::InvalidArgument("peer is empty".to_string()));
        }
        if let Some((endpoint, node_key)) = value.rsplit_once('=') {
            if endpoint.trim().is_empty() {
                return Err(MsgbusError::InvalidArgument(
                    "peer endpoint is empty".to_string(),
                ));
            }
            return Ok(Self {
                endpoint: endpoint.to_string(),
                expected_node: Some(NodeId::from_key(node_key)?),
            });
        }
        Ok(Self {
            endpoint: value,
            expected_node: None,
        })
    }
}

#[derive(Clone, Debug, Default)]
pub struct PeerClientTlsConfig {
    pub ca_cert: Option<PathBuf>,
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
    pub domain_name: Option<String>,
}

impl PeerClientTlsConfig {
    pub fn is_enabled(&self) -> bool {
        self.ca_cert.is_some()
            || self.cert.is_some()
            || self.key.is_some()
            || self.domain_name.is_some()
    }
}

#[derive(Clone, Debug)]
pub struct PeerSyncConfig {
    pub local_node: NodeId,
    pub peers: Vec<PeerSpec>,
    pub interval: Duration,
    pub batch_limit: u32,
    pub tls: Option<PeerClientTlsConfig>,
}

impl PeerSyncConfig {
    pub fn disabled(local_node: NodeId) -> Self {
        Self {
            local_node,
            peers: Vec::new(),
            interval: Duration::from_secs(1),
            batch_limit: 100,
            tls: None,
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
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    spawn_peer_sync_with_shutdown(store, published, config, shutdown_rx)
}

pub fn spawn_peer_sync_with_shutdown<S>(
    store: Arc<S>,
    published: broadcast::Sender<StoredMessage>,
    config: PeerSyncConfig,
    mut shutdown: watch::Receiver<bool>,
) -> JoinHandle<()>
where
    S: MsgbusStore,
{
    tokio::spawn(async move {
        for peer in &config.peers {
            if peer.expected_node.is_none() {
                warn!(
                    peer = peer.endpoint,
                    "peer identity is not pinned; use --peer endpoint=tenant/bl/device for binding"
                );
            }
        }
        let mut interval = tokio::time::interval(config.interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(err) = sync_once(store.clone(), published.clone(), &config).await {
                        warn!(error = %err, "peer sync pass failed");
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        tracing::info!("peer sync shutdown received");
                        break;
                    }
                }
            }
        }
    })
}

pub async fn sync_once<S>(
    store: Arc<S>,
    published: broadcast::Sender<StoredMessage>,
    config: &PeerSyncConfig,
) -> SyncResult<()>
where
    S: MsgbusStore,
{
    let mut first_error = None;

    for peer in &config.peers {
        if let Err(err) = sync_peer(
            store.clone(),
            published.clone(),
            &config.local_node,
            peer,
            config.batch_limit,
            config.tls.as_ref(),
        )
        .await
        {
            let error = err.to_string();
            warn!(peer = peer.endpoint, error, "peer sync failed");
            if let Err(record_err) =
                record_peer_sync_failure(store.as_ref(), peer, None, None, 0, 0, error.clone())
                    .await
            {
                warn!(
                    peer = peer.endpoint,
                    error = %record_err,
                    "failed to record peer sync failure"
                );
            }
            if first_error.is_none() {
                first_error = Some(error);
            }
        }
    }

    if let Some(error) = first_error {
        Err(error.into())
    } else {
        Ok(())
    }
}

async fn sync_peer<S>(
    store: Arc<S>,
    published: broadcast::Sender<StoredMessage>,
    local_node: &NodeId,
    peer: &PeerSpec,
    batch_limit: u32,
    tls: Option<&PeerClientTlsConfig>,
) -> SyncResult<()>
where
    S: MsgbusStore,
{
    let mut client = connect_peer(peer, tls).await?;
    verify_peer_identity(&mut client, local_node, peer).await?;
    let heads = client
        .list_heads(ListHeadsRequest {})
        .await?
        .into_inner()
        .heads;

    for head in heads {
        let head = core_topic_head(head).map_err(status_box)?;
        if let Err(err) = sync_head(
            store.clone(),
            published.clone(),
            &mut client,
            peer,
            &head,
            batch_limit,
        )
        .await
        {
            let local_head = store
                .get_head(&head.topic, &head.origin)
                .await?
                .unwrap_or(HeadId(0))
                .0;
            record_peer_sync_failure(
                store.as_ref(),
                peer,
                Some(head.topic.clone()),
                Some(head.origin.clone()),
                local_head,
                head.head_id.0,
                err.to_string(),
            )
            .await?;
            return Err(err);
        }
    }

    record_peer_sync_success(store.as_ref(), peer, None, None, 0, 0, 0).await?;

    Ok(())
}

async fn sync_head<S>(
    store: Arc<S>,
    published: broadcast::Sender<StoredMessage>,
    client: &mut MsgbusServiceClient<Channel>,
    peer: &PeerSpec,
    head: &TopicHead,
    batch_limit: u32,
) -> SyncResult<()>
where
    S: MsgbusStore,
{
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
                    "peer {} returned mismatched message for topic={} origin={}",
                    peer.endpoint,
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
                        peer = peer.endpoint,
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

        record_peer_sync_success(
            store.as_ref(),
            peer,
            Some(head.topic.clone()),
            Some(head.origin.clone()),
            local_head,
            head.head_id.0,
            local_head,
        )
        .await?;

        if fetched == 0 || local_head == before {
            break;
        }
    }

    if local_head >= head.head_id.0 {
        record_peer_sync_success(
            store.as_ref(),
            peer,
            Some(head.topic.clone()),
            Some(head.origin.clone()),
            local_head,
            head.head_id.0,
            local_head,
        )
        .await?;
    }
    Ok(())
}

async fn connect_peer(
    peer: &PeerSpec,
    tls: Option<&PeerClientTlsConfig>,
) -> SyncResult<MsgbusServiceClient<Channel>> {
    let mut endpoint = Endpoint::from_shared(peer.endpoint.clone())?;
    if let Some(tls) = tls.filter(|tls| tls.is_enabled()) {
        let mut config = ClientTlsConfig::new().with_enabled_roots();
        if let Some(ca_cert) = &tls.ca_cert {
            config = config.ca_certificate(Certificate::from_pem(std::fs::read(ca_cert)?));
        }
        match (&tls.cert, &tls.key) {
            (Some(cert), Some(key)) => {
                config = config.identity(Identity::from_pem(
                    std::fs::read(cert)?,
                    std::fs::read(key)?,
                ));
            }
            (None, None) => {}
            _ => {
                return Err("peer TLS cert and key must be configured together".into());
            }
        }
        if let Some(domain_name) = &tls.domain_name {
            config = config.domain_name(domain_name.clone());
        }
        endpoint = endpoint.tls_config(config)?;
    }
    let channel = endpoint.connect().await?;
    Ok(MsgbusServiceClient::new(channel))
}

async fn verify_peer_identity(
    client: &mut MsgbusServiceClient<Channel>,
    local_node: &NodeId,
    peer: &PeerSpec,
) -> SyncResult<()> {
    let response = client.health(HealthRequest {}).await?.into_inner();
    let remote_node = response
        .node
        .map(core_node)
        .transpose()
        .map_err(status_box)?
        .ok_or_else(|| format!("peer {} health response has no node", peer.endpoint))?;
    if remote_node == *local_node {
        return Err(format!("peer {} reports the local node identity", peer.endpoint).into());
    }
    if let Some(expected_node) = &peer.expected_node
        && &remote_node != expected_node
    {
        return Err(format!(
            "peer {} identity mismatch: expected {}, got {}",
            peer.endpoint,
            expected_node.key(),
            remote_node.key()
        )
        .into());
    }
    debug!(
        peer = peer.endpoint,
        remote_node = remote_node.key(),
        "peer identity verified"
    );
    Ok(())
}

async fn record_peer_sync_success<S>(
    store: &S,
    peer: &PeerSpec,
    topic: Option<msgbus_core::Topic>,
    origin: Option<NodeId>,
    local_head: u64,
    remote_head: u64,
    last_synced_head: u64,
) -> SyncResult<()>
where
    S: MsgbusStore,
{
    store
        .record_peer_sync_state(PeerSyncState {
            peer: peer.endpoint.clone(),
            topic,
            origin,
            local_head: HeadId(local_head),
            remote_head: HeadId(remote_head),
            last_synced_head: HeadId(last_synced_head),
            last_attempt_at_ms: now_ms(),
            last_success_at_ms: now_ms(),
            consecutive_failures: 0,
            last_error: None,
        })
        .await?;
    Ok(())
}

async fn record_peer_sync_failure<S>(
    store: &S,
    peer: &PeerSpec,
    topic: Option<msgbus_core::Topic>,
    origin: Option<NodeId>,
    local_head: u64,
    remote_head: u64,
    error: String,
) -> SyncResult<()>
where
    S: MsgbusStore,
{
    store
        .record_peer_sync_state(PeerSyncState {
            peer: peer.endpoint.clone(),
            topic,
            origin,
            local_head: HeadId(local_head),
            remote_head: HeadId(remote_head),
            last_synced_head: HeadId(local_head),
            last_attempt_at_ms: now_ms(),
            last_success_at_ms: 0,
            consecutive_failures: 0,
            last_error: Some(error),
        })
        .await?;
    Ok(())
}

fn status_box(status: Status) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(status)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_unpinned_peer() {
        let peer = PeerSpec::parse("http://127.0.0.1:50052").expect("peer");
        assert_eq!(peer.endpoint, "http://127.0.0.1:50052");
        assert!(peer.expected_node.is_none());
    }

    #[test]
    fn parses_identity_bound_peer() {
        let peer = PeerSpec::parse("https://node-b:50052=tenant/default/node-b").expect("peer");
        assert_eq!(peer.endpoint, "https://node-b:50052");
        assert_eq!(
            peer.expected_node.expect("expected node").key(),
            "tenant/default/node-b"
        );
    }
}
