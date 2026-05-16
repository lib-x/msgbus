use clap::{Parser, ValueEnum};
use msgbus_core::{MsgbusStore, NodeId};
use msgbus_server::sync::{
    PeerClientTlsConfig, PeerSpec, PeerSyncConfig, spawn_peer_sync_with_shutdown,
};
use msgbus_server::{MsgbusGrpcService, replay_control_messages};
use msgbus_store_redb::RedbStore;
use msgbus_store_sqlite::SqliteStore;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::watch;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
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

    #[arg(long, env = "MSGBUS_PEER_TLS_CA")]
    peer_tls_ca: Option<PathBuf>,

    #[arg(long, env = "MSGBUS_PEER_TLS_CERT")]
    peer_tls_cert: Option<PathBuf>,

    #[arg(long, env = "MSGBUS_PEER_TLS_KEY")]
    peer_tls_key: Option<PathBuf>,

    #[arg(long, env = "MSGBUS_PEER_TLS_DOMAIN")]
    peer_tls_domain: Option<String>,

    #[arg(long, env = "MSGBUS_TLS_CERT")]
    tls_cert: Option<PathBuf>,

    #[arg(long, env = "MSGBUS_TLS_KEY")]
    tls_key: Option<PathBuf>,

    #[arg(long, env = "MSGBUS_TLS_CLIENT_CA")]
    tls_client_ca: Option<PathBuf>,

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
    let node = NodeId::new(
        args.tenant_id.clone(),
        args.bl_name.clone(),
        args.device_id.clone(),
    )?;
    let peers = args
        .peers
        .iter()
        .map(PeerSpec::parse)
        .collect::<Result<Vec<_>, _>>()?;
    let peer_tls = peer_tls_config(&args)?;
    let server_tls = server_tls_config(&args)?;
    let sync = PeerSyncConfig {
        local_node: node.clone(),
        peers,
        interval: Duration::from_millis(args.sync_interval_ms.max(100)),
        batch_limit: args.sync_batch_limit.max(1),
        tls: peer_tls,
    };
    match args.storage {
        StorageKind::Redb => {
            let store = Arc::new(RedbStore::open(args.data)?);
            serve(args.listen, node, store, sync, server_tls).await?;
        }
        StorageKind::Sqlite => {
            let store = Arc::new(SqliteStore::open(args.data)?);
            serve(args.listen, node, store, sync, server_tls).await?;
        }
    }

    Ok(())
}

async fn serve<S>(
    listen: SocketAddr,
    node: NodeId,
    store: Arc<S>,
    sync: PeerSyncConfig,
    server_tls: Option<ServerTlsConfig>,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: MsgbusStore,
{
    replay_control_messages(store.as_ref()).await?;
    let service = MsgbusGrpcService::new(store.clone(), node);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let sync_task = if sync.is_enabled() {
        tracing::info!(
            peers = sync.peers.len(),
            interval_ms = sync.interval.as_millis(),
            "starting peer sync"
        );
        Some(spawn_peer_sync_with_shutdown(
            store,
            service.published_sender(),
            sync,
            shutdown_rx,
        ))
    } else {
        None
    };
    let service = service.into_server();

    tracing::info!(listen = %listen, "starting msgbusd");
    let mut server = Server::builder();
    if let Some(server_tls) = server_tls {
        server = server.tls_config(server_tls)?;
        tracing::info!("server TLS enabled");
    }
    server
        .add_service(service)
        .serve_with_shutdown(listen, async {
            if let Err(err) = shutdown_signal().await {
                tracing::warn!(error = %err, "shutdown signal listener failed");
            }
            tracing::info!("shutdown signal received");
            let _ = shutdown_tx.send(true);
        })
        .await?;

    if let Some(sync_task) = sync_task {
        let _ = tokio::time::timeout(Duration::from_secs(5), sync_task).await;
    }

    Ok(())
}

#[cfg(unix)]
async fn shutdown_signal() -> std::io::Result<()> {
    let mut terminate = signal::unix::signal(signal::unix::SignalKind::terminate())?;
    tokio::select! {
        result = signal::ctrl_c() => result,
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> std::io::Result<()> {
    signal::ctrl_c().await
}

fn peer_tls_config(args: &Args) -> Result<Option<PeerClientTlsConfig>, Box<dyn std::error::Error>> {
    match (&args.peer_tls_cert, &args.peer_tls_key) {
        (Some(_), Some(_)) | (None, None) => {}
        _ => return Err("peer TLS cert and key must be configured together".into()),
    }
    let config = PeerClientTlsConfig {
        ca_cert: args.peer_tls_ca.clone(),
        cert: args.peer_tls_cert.clone(),
        key: args.peer_tls_key.clone(),
        domain_name: args.peer_tls_domain.clone(),
    };
    Ok(config.is_enabled().then_some(config))
}

fn server_tls_config(args: &Args) -> Result<Option<ServerTlsConfig>, Box<dyn std::error::Error>> {
    match (&args.tls_cert, &args.tls_key) {
        (Some(cert), Some(key)) => {
            let mut config = ServerTlsConfig::new().identity(Identity::from_pem(
                std::fs::read(cert)?,
                std::fs::read(key)?,
            ));
            if let Some(client_ca) = &args.tls_client_ca {
                config = config.client_ca_root(Certificate::from_pem(std::fs::read(client_ca)?));
            }
            Ok(Some(config))
        }
        (None, None) => {
            if args.tls_client_ca.is_some() {
                return Err(
                    "MSGBUS_TLS_CLIENT_CA requires MSGBUS_TLS_CERT and MSGBUS_TLS_KEY".into(),
                );
            }
            Ok(None)
        }
        _ => Err("server TLS cert and key must be configured together".into()),
    }
}
