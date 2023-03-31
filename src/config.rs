use crate::errors::ZkError;
use clap::*;
use serde_derive::Deserialize;
use std::path::PathBuf;

#[derive(Debug)]
pub struct CmdArg {
    pub(crate) config_path: PathBuf,
}

pub fn get_args() -> CmdArg {
    let matches = command!()
        .arg(
            arg!(
                -c --config <FILE> "Sets a custom config file"
            )
                // We don't have syntax yet for optional options, so manually calling `required`
                .required(true)
                .value_parser(value_parser!(PathBuf)),
        )
        .arg(arg!(
            -d --debug ... "Turn debugging information on"
        ))
        .get_matches();
    let config_path = matches.get_one::<PathBuf>("config").unwrap().clone();
    CmdArg { config_path }
}

#[derive(Debug, Deserialize, Clone)]
pub struct Server {
    pub address: String,
    pub port: u16,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Upstream {
    pub address: String,
    pub port: u16,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CircuitBreak {
    name: String,
    pub regex: String,
    pub opcode: Vec<i32>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub(crate) server: Server,
    pub(crate) upstream: Upstream,
    pub circuit_break: Vec<CircuitBreak>,
}

pub async fn parse_config_file(config_path: &PathBuf) -> Result<Config, ZkError> {
    let toml_content = tokio::fs::read_to_string(config_path)
        .await
        .map_err(|_| ZkError::IoError)?;
    let config: Config = toml::from_str(&toml_content).map_err(|_| ZkError::InternalError)?;
    Ok(config)
}
