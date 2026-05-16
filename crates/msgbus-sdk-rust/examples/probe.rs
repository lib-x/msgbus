use msgbus_sdk_rust::{MsgbusClient, NodeId};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint =
        std::env::var("MSGBUS_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());
    let action = std::env::var("MSGBUS_ACTION").unwrap_or_else(|_| "list-heads".to_string());
    let topic = std::env::var("MSGBUS_TOPIC").unwrap_or_else(|_| "demo.events".to_string());
    let payload = std::env::var("MSGBUS_PAYLOAD").unwrap_or_else(|_| "hello msgbus".to_string());
    let origin = NodeId {
        tenant_id: std::env::var("MSGBUS_ORIGIN_TENANT").unwrap_or_else(|_| "tenant".to_string()),
        bl_name: std::env::var("MSGBUS_ORIGIN_BL").unwrap_or_else(|_| "default".to_string()),
        device_id: std::env::var("MSGBUS_ORIGIN_DEVICE").unwrap_or_else(|_| "device".to_string()),
    };

    let mut client = MsgbusClient::connect(endpoint)
        .await?
        .with_default_origin(origin.clone());

    match action.as_str() {
        "publish" => {
            let message = client.publish(topic, payload.into_bytes()).await?;
            println!("published head_id={}", message.head_id);
        }
        "fetch" => {
            let messages = client.fetch(topic, Some(origin), 1, 100).await?;
            println!("fetched={}", messages.len());
            for message in messages {
                println!(
                    "message head_id={} deleted={} payload={}",
                    message.head_id,
                    message.deleted,
                    String::from_utf8_lossy(&message.payload)
                );
            }
        }
        "delete" => {
            let from = std::env::var("MSGBUS_FROM_HEAD")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(1);
            let to = std::env::var("MSGBUS_TO_HEAD")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(from);
            let deleted = client.delete_range(topic, Some(origin), from, to).await?;
            println!("deleted={deleted}");
        }
        "list-heads" => {
            for head in client.list_heads().await? {
                let Some(origin) = head.origin else {
                    continue;
                };
                println!(
                    "head topic={} origin={}/{}/{} head_id={}",
                    head.topic, origin.tenant_id, origin.bl_name, origin.device_id, head.head_id
                );
            }
        }
        "list-sync" => {
            for state in client.list_peer_sync_states().await? {
                let origin = state
                    .origin
                    .map(|origin| {
                        format!(
                            "{}/{}/{}",
                            origin.tenant_id, origin.bl_name, origin.device_id
                        )
                    })
                    .unwrap_or_else(|| "-".to_string());
                let last_error = if state.last_error.is_empty() {
                    "-"
                } else {
                    state.last_error.as_str()
                };
                println!(
                    "sync peer={} topic={} origin={} local_head={} remote_head={} last_synced_head={} failures={} last_error={}",
                    state.peer,
                    state.topic,
                    origin,
                    state.local_head,
                    state.remote_head,
                    state.last_synced_head,
                    state.consecutive_failures,
                    last_error
                );
            }
        }
        other => {
            return Err(format!("unsupported MSGBUS_ACTION: {other}").into());
        }
    }

    Ok(())
}
