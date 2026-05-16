use msgbus_core::{FetchQuery, HeadId, MsgbusStore, NewMessage, NodeId, Topic};
use msgbus_server::MsgbusGrpcService;
use msgbus_server::sync::{PeerSpec, PeerSyncConfig, sync_once};
use msgbus_store_sqlite::SqliteStore;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

const DELETE_TOPIC: &str = "$msgbus.delete";

#[tokio::test]
async fn sync_once_replicates_messages_and_delete_markers() {
    let local_node = NodeId::new("tenant", "bl", "local").expect("local node");
    let remote_node = NodeId::new("tenant", "bl", "remote").expect("remote node");
    let local_store = Arc::new(SqliteStore::open_in_memory().expect("local store"));
    let remote_store = Arc::new(SqliteStore::open_in_memory().expect("remote store"));
    let topic = Topic::new("events").expect("topic");

    remote_store
        .append_message(NewMessage::new(
            topic.clone(),
            remote_node.clone(),
            b"one".to_vec(),
            BTreeMap::new(),
        ))
        .await
        .expect("append remote message");

    let (remote_endpoint, stop_remote) =
        spawn_server(remote_store.clone(), remote_node.clone()).await;
    let local_service = MsgbusGrpcService::new(local_store.clone(), local_node.clone());
    let config = PeerSyncConfig {
        local_node,
        peers: vec![PeerSpec {
            endpoint: remote_endpoint,
            expected_node: Some(remote_node.clone()),
        }],
        interval: Duration::from_secs(1),
        batch_limit: 10,
        tls: None,
    };

    sync_once(
        local_store.clone(),
        local_service.published_sender(),
        &config,
    )
    .await
    .expect("initial sync");
    let messages = local_store
        .fetch_messages(FetchQuery {
            topic: topic.clone(),
            origin: remote_node.clone(),
            from_head_id: HeadId(1),
            limit: 10,
        })
        .await
        .expect("fetch local replicated messages");
    assert_eq!(messages.len(), 1);
    assert!(!messages[0].deleted);

    remote_store
        .tombstone_range_with_marker(
            &topic,
            &remote_node,
            HeadId(1),
            HeadId(1),
            &Topic::new(DELETE_TOPIC).expect("delete topic"),
            &remote_node,
        )
        .await
        .expect("delete remote message");

    sync_once(
        local_store.clone(),
        local_service.published_sender(),
        &config,
    )
    .await
    .expect("delete marker sync");
    let messages = local_store
        .fetch_messages(FetchQuery {
            topic,
            origin: remote_node,
            from_head_id: HeadId(1),
            limit: 10,
        })
        .await
        .expect("fetch deleted local message");
    assert_eq!(messages.len(), 1);
    assert!(messages[0].deleted);

    let _ = stop_remote.send(());
}

async fn spawn_server(store: Arc<SqliteStore>, node: NodeId) -> (String, oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test server");
    let addr = listener.local_addr().expect("test server address");
    let incoming = TcpListenerStream::new(listener);
    let (stop_tx, stop_rx) = oneshot::channel();
    let service = MsgbusGrpcService::new(store, node).into_server();

    tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stop_rx.await;
            })
            .await
            .expect("test server");
    });

    (format!("http://{addr}"), stop_tx)
}
