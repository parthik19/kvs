use kvs::KvStore;
use kvs::Result;
use std::env;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
// #[structopt(version = env!(CARGO_PKG_VERSION))]
enum InputCommand {
    Set { key: String, value: String },
    Get { key: String },
    Rm { key: String },
}

fn main() -> Result<()> {
    let input_command = InputCommand::from_args();

    let mut kvstore = KvStore::open(env::current_dir()?)?;

    match input_command {
        InputCommand::Set { key, value } => kvstore.set(key, value)?,
        InputCommand::Get { key } => {
            if let Ok(Some(value)) = kvstore.get(key.to_string()) {
                println!("{}", value);
            } else {
                println!("Key not found");
                std::process::exit(0);
            }
        }
        InputCommand::Rm { key } => {
            if let Err(_) = kvstore.remove(key) {
                println!("Key not found");
                std::process::exit(1);
            }
        }
    }

    Ok(())
}
