//! This is a crate used to maintain an in memory key value database

// #![deny(missing_docs)]
use failure::format_err;
use serde::{Deserialize, Serialize};
use serde_json;
use sled;

use std::str::FromStr;
use std::{
    collections::HashMap,
    env, fs,
    fs::OpenOptions,
    io::{BufRead, BufReader, BufWriter, Read, Seek, Write},
    net::TcpStream,
    u64,
};
use std::{fs::File, path::PathBuf};
use std::{
    io,
    net::{SocketAddr, TcpListener},
};

/// Whether command worked successfully
pub type Result<T> = std::result::Result<T, failure::Error>;

const COMPACTION_THRESHOLD: f32 = 0.5;

#[derive(Debug, Serialize, Deserialize)]
/// the command the kvs engine will execute
pub enum Command {
    /// set a value for a key
    Set { key: String, value: String },

    /// retrieve a value for a key
    Get { key: String },

    /// remove a key/value pairing
    Remove { key: String },
}

#[derive(Serialize, Deserialize)]
pub enum ServerResponse {
    GetResponse(Option<String>),
    RemoveSuccess,
    RemoveFailure,
    SetSuccess,
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

/// holds the key value pairings
pub struct KvStore {
    log_writer: BufWriterWithPosition<File>,
    log_reader: BufReader<File>,
    index: HashMap<String, CommandPos>,
    num_unnecessary_entries: usize,
    path: PathBuf, // the path it was initially opened with
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path: PathBuf = path.into();

        fs::create_dir_all(&path)?;

        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path.join(format!("kvs.log")))?;

        let mut index = HashMap::new();

        let mut num_unnecessary_entries = 0;

        let log_reader = BufReader::new(log_file.try_clone()?);
        let mut bytes_read = 0;
        for line in log_reader.lines() {
            let line = line?;

            let cmd: Command = serde_json::from_str(&line)?;
            let cmd_len = line.len() as u64;

            match cmd {
                Command::Set { key, value: _ } => {
                    if index.contains_key(&key) {
                        num_unnecessary_entries += 1;
                    }

                    index.insert(
                        key,
                        CommandPos {
                            pos: bytes_read,
                            len: cmd_len,
                        },
                    )
                }
                Command::Remove { key } => index.remove(&key),
                Command::Get { key: _ } => unreachable!(),
            };

            bytes_read += cmd_len + 1; // because of the newline separating commands
        }

        Ok(Self {
            log_writer: BufWriterWithPosition::new(log_file.try_clone()?, bytes_read),
            log_reader: BufReader::new(log_file),
            index,
            num_unnecessary_entries,
            path,
        })
    }

    fn should_compact(&self) -> bool {
        self.num_unnecessary_entries as f32 / self.index.len() as f32 > COMPACTION_THRESHOLD
    }

    fn compact(&mut self) -> Result<()> {
        let mut new_log_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(self.path.join("kvs_temp.log"))?;

        for (_, command_pos) in self.index.iter() {
            let local_reader = &mut self.log_reader;

            local_reader.seek(io::SeekFrom::Start(command_pos.pos))?; // offset reader's cursor to start of the desired command
            let mut cmd_reader = local_reader.take(command_pos.len);

            let mut command = String::new();
            cmd_reader.read_to_string(&mut command)?;

            if let command @ Command::Set { key: _, value: _ } = serde_json::from_str(&command)? {
                // write to temp log file
                serde_json::to_writer(&mut new_log_file, &command)?;
                write!(&mut new_log_file, "\n")?;
            } else {
                panic!(
                    "When compacting, index did of a key did not point to a SET command in the log"
                )
            }
        }

        new_log_file.flush()?;

        // don't need the old log file now, rename to kvs.log thereby replacing the old log file
        fs::rename(self.path.join("kvs_temp.log"), self.path.join("kvs.log"))?;

        let mut new_store = Self::open(&self.path)?;

        std::mem::swap(self, &mut new_store);

        Ok(())
    }
}

impl KvsEngine for KvStore {
    fn get(&mut self, key: String) -> Result<Option<String>> {
        let command_pos = self.index.get(&key);

        if let Some(command_pos) = command_pos {
            let local_reader = &mut self.log_reader;

            local_reader.seek(io::SeekFrom::Start(command_pos.pos))?; // offset reader's cursor to start of the desired command
            let mut cmd_reader = local_reader.take(command_pos.len);

            let mut command = String::new();
            cmd_reader.read_to_string(&mut command)?;

            if let Command::Set { key: _, value } = serde_json::from_str(&command)? {
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        if self.index.contains_key(&key) {
            self.num_unnecessary_entries += 1;
        }

        let command = Command::Set {
            key: key.clone(),
            value: value.clone(),
        };

        let num_bytes_written_before_write = self.log_writer.num_bytes_written;

        serde_json::to_writer(&mut self.log_writer, &command)?;
        self.log_writer.write(b"\n")?;

        let num_bytes_written_after_write = self.log_writer.num_bytes_written;

        let command_pos = CommandPos {
            pos: num_bytes_written_before_write,
            len: num_bytes_written_after_write - num_bytes_written_before_write,
        };

        self.index.insert(key, command_pos);

        if self.should_compact() {
            self.compact()?;
        }

        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if self.get(key.clone())?.is_some() {
            let command = Command::Remove { key: key.clone() };
            serde_json::to_writer(&mut self.log_writer, &command)?;
            // writeln!(&mut self.log_writer)?;
            self.log_writer.write(b"\n")?;

            self.index.remove(&key);

            Ok(())
        } else {
            Err(format_err!("Key not found"))
        }
    }
}

struct BufWriterWithPosition<T>
where
    T: Write + Read,
{
    writer: BufWriter<T>,
    num_bytes_written: u64,
}

impl<T> BufWriterWithPosition<T>
where
    T: Write + Read,
{
    fn new(w: T, num_bytes_written: u64) -> Self {
        Self {
            writer: BufWriter::new(w),
            num_bytes_written,
        }
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        self.log_writer
            .flush()
            .expect("Failed flushing log_writer when dropping KvStore");
    }
}

impl<T> Write for BufWriterWithPosition<T>
where
    T: Write + Read,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let bytes_written = self.writer.write(buf)?;
        self.writer.flush()?;

        self.num_bytes_written += bytes_written as u64;

        Ok(bytes_written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

// TODO functionality for kvs-client to talk to kvs-server
pub struct KvsClient {
    server_addr: SocketAddr,
}

impl KvsClient {
    pub fn with_addr(addr: SocketAddr) -> Self {
        Self { server_addr: addr }
    }

    pub fn send_command(&self, command: Command) -> Result<Option<String>> {
        // append newline char because server reads bytes up to a new line per command
        let command_string = format!("{}\n", serde_json::to_string(&command)?);
        let command_bytes = command_string.as_bytes();

        let mut tcp_stream = TcpStream::connect(self.server_addr)?;

        tcp_stream.write(command_bytes)?;

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
                Box::new(KvStore::open(env::current_dir()?)?)
            }
            (Some(EngineType::Sled), Some(EngineType::Sled)) => {
                Box::new(SledKvsEngine::open(env::current_dir()?)?)
            }
            (Some(EngineType::Kvs), None) => Box::new(KvStore::open(env::current_dir()?)?),
            (Some(EngineType::Sled), None) => Box::new(SledKvsEngine::open(env::current_dir()?)?),
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
            None | Some(EngineType::Kvs) => Ok(Box::new(KvStore::open(env::current_dir()?)?)),
            Some(EngineType::Sled) => Ok(Box::new(SledKvsEngine::open(env::current_dir()?)?)),
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

        eprintln!("Didn't find any log files when searching for existing engine");
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

                stream.write(server_response.as_bytes())?;
                Ok(())
            }
            Command::Set { key, value } => {
                let server_response = if let Ok(_) = self.engine.set(key, value) {
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

                stream.write(server_response.as_bytes())?;
                Ok(())
            }
        }
    }
}

// sled storage stuff starts here
struct SledKvsEngine {
    inner: sled::Db,
}

impl SledKvsEngine {
    fn open(path: PathBuf) -> Result<SledKvsEngine> {
        Ok(Self {
            inner: sled::open(path.join("sled_db.log"))?,
        })
    }
}

impl KvsEngine for SledKvsEngine {
    fn get(&mut self, key: String) -> Result<Option<String>> {
        Ok(self
            .inner
            .get(key)?
            .map(|i_vec| AsRef::<[u8]>::as_ref(&i_vec).to_vec())
            .map(String::from_utf8)
            .transpose()?)
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.inner.insert(key, value.into_bytes()).map(|_| ())?;
        self.inner.flush()?;
        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        let result = self.inner.remove(key)?;
        self.inner.flush()?;

        if let Some(_) = result {
            Ok(())
        } else {
            Err(format_err!("Removing non existent key"))
        }
    }
}

#[derive(Debug)]
pub enum EngineType {
    Kvs,
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
