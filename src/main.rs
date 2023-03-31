use crate::config::*;
use crate::errors::ZkError;
use crate::server::ZkServer;
use log::info;

mod codec;
mod config;
mod constants;
mod errors;
mod length_codec;
mod proto;
mod record;
mod server;
mod zk_errcode;

#[tokio::main]
async fn main() -> Result<(), ZkError> {
    info!("zookeeper proxy server init...");
    env_logger::init();
    let cmd_args = get_args();
    info!("Args: {:?}", cmd_args);
    let config = parse_config_file(&cmd_args.config_path).await?;
    info!("Config: {:?}", config);

    let zk_server = ZkServer::new(config);
    zk_server.start().await
}
