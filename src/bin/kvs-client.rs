use kvs::Command;
use kvs::{KvsClient, Result};
use std::net::SocketAddr;
use structopt::StructOpt;

fn main() -> Result<()> {
    let client_command = KvsClientCommand::from_args();
    let command: Command = Command::from(&client_command);

    match client_command {
        KvsClientCommand::Set {
            key: _,
            value: _,
            addr,
        } => {
            let kvs_client = KvsClient::with_addr(addr);
            kvs_client.send_command(command)?;
        }
        KvsClientCommand::Get { key: _, addr } => {
            let kvs_client = KvsClient::with_addr(addr);
            let get_result = kvs_client.send_command(command)?;

            if let Some(existing_get_result) = get_result {
                println!("{}", existing_get_result);
            } else {
                println!("Key not found");
            }
        }
        KvsClientCommand::Rm { key: _, addr } => {
            let kvs_client = KvsClient::with_addr(addr);
            if let Err(_) = kvs_client.send_command(command) {
                eprintln!("Key not found");
                std::process::exit(1);
            }
        }
    }

    Ok(())
}

#[derive(StructOpt, Debug)]
pub enum KvsClientCommand {
    Set {
        key: String,
        value: String,

        #[structopt(long, default_value = "127.0.0.1:4000")]
        addr: SocketAddr,
    },
    Get {
        key: String,

        #[structopt(long, default_value = "127.0.0.1:4000")]
        addr: SocketAddr,
    },
    Rm {
        key: String,

        #[structopt(long, default_value = "127.0.0.1:4000")]
        addr: SocketAddr,
    },
}

impl From<&KvsClientCommand> for Command {
    fn from(kcc: &KvsClientCommand) -> Self {
        match kcc {
            KvsClientCommand::Set {
                key,
                value,
                addr: _,
            } => Command::Set {
                key: key.to_owned(),
                value: value.to_owned(),
            },
            KvsClientCommand::Get { key, addr: _ } => Command::Get {
                key: key.to_owned(),
            },
            KvsClientCommand::Rm { key, addr: _ } => Command::Remove {
                key: key.to_owned(),
            },
        }
    }
}
