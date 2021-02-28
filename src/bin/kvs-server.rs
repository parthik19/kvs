use kvs::{KvsServer, Result};
use log::info;
use simplelog::{Config, LevelFilter, TermLogger, TerminalMode};
use std::net::SocketAddr;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct KvsServerCommand {
    #[structopt(long = "addr", default_value = "127.0.0.1:4000")]
    addr: SocketAddr,

    #[structopt(long = "engine")]
    engine: String,
}

fn main() -> Result<()> {
    TermLogger::init(LevelFilter::Info, Config::default(), TerminalMode::Stderr)?;

    let server_command = KvsServerCommand::from_args();

    info!("version: {}", env!("CARGO_PKG_VERSION"));
    info!(
        "{} running on {}",
        server_command.engine, server_command.addr
    );

    let kvs_server = KvsServer::with_addr(server_command.addr)?;

    kvs_server.run()
}
