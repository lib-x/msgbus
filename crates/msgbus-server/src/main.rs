use clap::{Parser, ValueEnum};
use msgbus_core::{MsgbusStore, NodeId};
use msgbus_server::MsgbusGrpcService;
use msgbus_server::sync::{PeerSyncConfig, spawn_peer_sync};
use msgbus_store_redb::RedbStore;
use msgbus_store_sqlite::SqliteStore;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tonic::transport::Server;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "msgbusd")]
#[command(about = "Rust msgbus daemon")]
struct Args {
    #[arg(long, env = "MSGBUS_LISTEN", default_value = "127.0.0.1:50051")]
    listen: SocketAddr,

    #[arg(long, env = "MSGBUS_DATA", default_value = "./msgbus.redb")]
    data: PathBuf,

    #[arg(long, env = "MSGBUS_STORAGE", default_value = "redb")]
    storage: StorageKind,

    #[arg(long = "peer", env = "MSGBUS_PEERS", value_delimiter = ',')]
    peers: Vec<String>,

    #[arg(long, env = "MSGBUS_SYNC_INTERVAL_MS", default_value_t = 1000)]
    sync_interval_ms: u64,

    #[arg(long, env = "MSGBUS_SYNC_BATCH_LIMIT", default_value_t = 100)]
    sync_batch_limit: u32,

    #[arg(long, env = "MSGBUS_TENANT_ID", default_value = "tenant")]
    tenant_id: String,

    #[arg(long, env = "MSGBUS_BL_NAME", default_value = "default")]
    bl_name: String,

    #[arg(long, env = "MSGBUS_DEVICE_ID", default_value = "device")]
    device_id: String,
}

#[derive(Clone, Debug, ValueEnum)]
enum StorageKind {
    Redb,
    Sqlite,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let args = Args::parse();
    let node = NodeId::new(args.tenant_id, args.bl_name, args.device_id)?;
    let sync = PeerSyncConfig {
        peers: args.peers,
        interval: Duration::from_millis(args.sync_interval_ms.max(100)),
        batch_limit: args.sync_batch_limit.max(1),
    };
    match args.storage {
        StorageKind::Redb => {
            let store = Arc::new(RedbStore::open(args.data)?);
            serve(args.listen, node, store, sync).await?;
        }
        StorageKind::Sqlite => {
            let store = Arc::new(SqliteStore::open(args.data)?);
            serve(args.listen, node, store, sync).await?;
        }
    }

    Ok(())
}

async fn serve<S>(
    listen: SocketAddr,
    node: NodeId,
    store: Arc<S>,
    sync: PeerSyncConfig,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: MsgbusStore,
{
    let service = MsgbusGrpcService::new(store.clone(), node);
    if sync.is_enabled() {
        tracing::info!(
            peers = sync.peers.len(),
            interval_ms = sync.interval.as_millis(),
            "starting peer sync"
        );
        let _sync_task = spawn_peer_sync(store, service.published_sender(), sync);
    }
    let service = service.into_server();

    tracing::info!(listen = %listen, "starting msgbusd");
    Server::builder()
        .add_service(service)
        .serve_with_shutdown(listen, async {
            let _ = signal::ctrl_c().await;
            tracing::info!("shutdown signal received");
        })
        .await?;

    Ok(())
}
