use kvs::{EngineType, KvsServer, Result};
use log::info;
use simplelog::{Config, LevelFilter, TermLogger, TerminalMode};
use std::net::SocketAddr;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct KvsServerCommand {
    #[structopt(long = "addr", default_value = "127.0.0.1:4000")]
    addr: SocketAddr,

    #[structopt(long = "engine")]
    engine: Option<EngineType>,
}

fn main() -> Result<()> {
    TermLogger::init(LevelFilter::Info, Config::default(), TerminalMode::Stderr)?;

    let server_command = KvsServerCommand::from_args();

    let kvs_server = KvsServer::new(server_command.addr, &server_command.engine)?;

    info!("version: {}", env!("CARGO_PKG_VERSION"));
    info!(
        "Engine {:?} running on {:?}",
        server_command.engine.unwrap_or(EngineType::Kvs).to_string(),
        server_command.addr
    );

    kvs_server.run()
}
