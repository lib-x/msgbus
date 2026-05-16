use clap::{Parser, ValueEnum};
use msgbus_core::{MsgbusStore, NodeId};
use msgbus_server::MsgbusGrpcService;
use msgbus_store_redb::RedbStore;
use msgbus_store_sqlite::SqliteStore;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
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
    match args.storage {
        StorageKind::Redb => {
            let store = Arc::new(RedbStore::open(args.data)?);
            serve(args.listen, node, store).await?;
        }
        StorageKind::Sqlite => {
            let store = Arc::new(SqliteStore::open(args.data)?);
            serve(args.listen, node, store).await?;
        }
    }

    Ok(())
}

async fn serve<S>(
    listen: SocketAddr,
    node: NodeId,
    store: Arc<S>,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: MsgbusStore,
{
    let service = MsgbusGrpcService::new(store, node).into_server();

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
