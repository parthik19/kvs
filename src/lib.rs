//! This is a crate used to maintain an in memory key value database

#![deny(missing_docs)]

use failure::format_err;
use serde::{Deserialize, Serialize};
use std::net::{SocketAddr, TcpListener};
use std::str::FromStr;
use std::{
    env, fs,
    io::{BufRead, BufReader, Read, Write},
    net::TcpStream,
    u64,
};

mod engines;

pub use engines::kvs::KvStore;
pub use engines::sled::SledKvsEngine;

/// Whether command worked successfully
pub type Result<T> = std::result::Result<T, failure::Error>;

const COMPACTION_THRESHOLD: f32 = 0.5;

#[derive(Debug, Serialize, Deserialize)]
/// the command the kvs engine will execute
pub enum Command {
    /// set a value for a key
    Set {
        /// key of KV pair to insert
        key: String,
        /// value of KV pair to insert
        value: String,
    },

    /// retrieve a value for a key
    Get {
        /// want value of this key
        key: String,
    },

    /// remove a key/value pairing
    Remove {
        /// remove KV pair of this key
        key: String,
    },
}

#[derive(Serialize, Deserialize)]
/// the response sent back from server to client
pub enum ServerResponse {
    /// get response
    GetResponse(Option<String>),

    /// returned when removal of a key was successful
    RemoveSuccess,

    /// returned when removal of a key was a failure
    RemoveFailure,

    /// returned when setting a KV pair was successful
    SetSuccess,

    /// returned when setting a KV pair was a failure
    SetFailure,
}

/// where in the log file the value resides
#[derive(Debug)]
pub struct CommandPos {
    pos: u64, // where the command starts in the file in bytes
    len: u64, // length of the command in bytes
}

/// defines the storage interface called by KvsServer
pub trait KvsEngine {
    /// gets the value associated with a key
    fn get(&mut self, key: String) -> Result<Option<String>>;

    /// set a key-value, overriding previous value if present
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /// removes a key and it's value
    fn remove(&mut self, key: String) -> Result<()>;
}

/// this struct exposes the interface for interacting with the KVS server
pub struct KvsClient {
    server_addr: SocketAddr,
}

impl KvsClient {
    /// create a KvsClient that listens to the specified port
    pub fn with_addr(addr: SocketAddr) -> Self {
        Self { server_addr: addr }
    }

    /// sends specified command to server
    pub fn send_command(&self, command: Command) -> Result<Option<String>> {
        // append newline char because server reads bytes up to a new line per command
        let command_string = format!("{}\n", serde_json::to_string(&command)?);
        let command_bytes = command_string.as_bytes();

        let mut tcp_stream = TcpStream::connect(self.server_addr)?;

        tcp_stream.write_all(command_bytes)?;

        let mut server_response = String::new();
        tcp_stream.read_to_string(&mut server_response)?;

        let server_response: ServerResponse = serde_json::from_str(&server_response)?;

        match server_response {
            ServerResponse::GetResponse(x) => Ok(x),
            ServerResponse::RemoveFailure => Err(format_err!("Key not found")),
            _ => Ok(None),
        }
    }
}

/// provides functionality to serve responses from server to client
pub struct KvsServer {
    listener: Option<TcpListener>,
    engine: Box<dyn KvsEngine>,
}

impl KvsServer {
    /// creates a KvsServer that listens on provided port
    pub fn new(addr: SocketAddr, engine: &Option<EngineType>) -> Result<Self> {
        let existing_engine = Self::existing_engine()?;
        let engine = match (&engine, &existing_engine) {
            (None, _) => Self::load_existing_or_default_engine(existing_engine)?,
            (Some(EngineType::Kvs), Some(EngineType::Kvs)) => {
                Box::new(engines::kvs::KvStore::open(env::current_dir()?)?)
            }
            (Some(EngineType::Sled), Some(EngineType::Sled)) => {
                Box::new(engines::sled::SledKvsEngine::open(env::current_dir()?)?)
            }
            (Some(EngineType::Kvs), None) => {
                Box::new(engines::kvs::KvStore::open(env::current_dir()?)?)
            }
            (Some(EngineType::Sled), None) => {
                Box::new(engines::sled::SledKvsEngine::open(env::current_dir()?)?)
            }
            _ => {
                return Err(format_err!(
                    "Incompatible engine specified: user: {:?}, existing: {:?}",
                    engine,
                    existing_engine
                ))
            }
        };

        Ok(Self {
            listener: Some(TcpListener::bind(addr)?),
            engine,
        })
    }

    fn load_existing_or_default_engine(
        existing_engine: Option<EngineType>,
    ) -> Result<Box<dyn KvsEngine>> {
        match existing_engine {
            None | Some(EngineType::Kvs) => {
                Ok(Box::new(engines::kvs::KvStore::open(env::current_dir()?)?))
            }
            Some(EngineType::Sled) => Ok(Box::new(engines::sled::SledKvsEngine::open(
                env::current_dir()?,
            )?)),
        }
    }

    fn existing_engine() -> Result<Option<EngineType>> {
        for entry in fs::read_dir(env::current_dir()?)? {
            let entry = entry?;
            let path = entry.path();
            if path.ends_with("kvs.log") {
                return Ok(Some(EngineType::Kvs));
            } else if path.ends_with("sled_db.log") {
                return Ok(Some(EngineType::Sled));
            }
        }

        Ok(None)
    }

    /// infinitely listens for incoming requests and executes them
    pub fn run(mut self) -> Result<()> {
        let listener = self
            .listener
            .take()
            .expect("KvsServer created without TCP listener!");

        for stream in listener.incoming() {
            self.handle_client_request(stream?)?;
        }

        Err(format_err!(
            "`incoming` loop broke on listener! Not listening on socket anymore."
        ))
    }

    // TODO return success message over TCP stream
    fn handle_client_request(&mut self, mut stream: TcpStream) -> Result<()> {
        let mut buf_reader = BufReader::new(&mut stream);

        let mut command = String::new(); // TODO initialize enough space for the smallest of get/set commands
        buf_reader.read_line(&mut command)?;

        let command: Command = serde_json::from_str(&command)?;

        match command {
            Command::Get { key } => {
                let result = self.engine.get(key)?;

                let server_response = ServerResponse::GetResponse(result);
                let server_response = serde_json::to_string(&server_response)?;
                let server_response = format!("{}\n", server_response);

                stream.write_all(server_response.as_bytes())?;
                Ok(())
            }
            Command::Set { key, value } => {
                let server_response = if self.engine.set(key, value).is_ok() {
                    ServerResponse::SetSuccess
                } else {
                    ServerResponse::SetFailure
                };

                let server_response = serde_json::to_string(&server_response)?;
                let server_response = format!("{}\n", server_response);

                stream.write(server_response.as_bytes())?;
                Ok(())
            }
            Command::Remove { key } => {
                let server_response = if let Ok(_) = self.engine.remove(key) {
                    ServerResponse::RemoveSuccess
                } else {
                    ServerResponse::RemoveFailure
                };

                let server_response = serde_json::to_string(&server_response)?;
                let server_response = format!("{}\n", server_response);

                stream.write_all(server_response.as_bytes())?;
                Ok(())
            }
        }
    }
}

/// the type of key value storage engine
#[derive(Debug)]
pub enum EngineType {
    /// custom in house definition
    Kvs,

    /// 3rd party KV storage engine
    Sled,
}

impl ToString for EngineType {
    fn to_string(&self) -> String {
        match self {
            Self::Kvs => String::from("kvs"),
            Self::Sled => String::from("sled"),
        }
    }
}

impl FromStr for EngineType {
    type Err = failure::Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "kvs" => Ok(Self::Kvs),
            "sled" => Ok(Self::Sled),
            _ => Err(format_err!("invalid engine type")),
        }
    }
}
