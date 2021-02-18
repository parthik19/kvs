//! This is a crate used to maintain an in memory key value database

#![deny(missing_docs)]

use failure::format_err;
use serde::{Deserialize, Serialize};
use serde_json;
use std::io;
use std::{
    collections::HashMap,
    fs,
    fs::OpenOptions,
    io::{BufRead, BufReader, BufWriter, Read, Seek, Write},
    u64,
};
use std::{fs::File, path::PathBuf};

/// holds the key value pairings
pub struct KvStore {
    log_writer: BufWriterWithPosition<File>,
    log_reader: BufReader<File>,
    /// index of the thing
    pub index: HashMap<String, CommandPos>,
}

#[derive(Serialize, Deserialize)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

/// where in the log file the value resides
#[derive(Debug)]
pub struct CommandPos {
    pos: u64, // where the command starts in the file in bytes
    len: u64, // length of the command in bytes
}

impl KvStore {
    /// retrieves the value of a key, if it exists. else None
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
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

    /// sets a key to the passed in value
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = Command::Set {
            key: key.clone(),
            value: value.clone(),
        };

        let num_bytes_written_before_write = self.log_writer.num_bytes_written;

        serde_json::to_writer(&mut self.log_writer, &command)?;
        // writeln!(&mut self.log_writer)?;
        self.log_writer.write(b"\n")?;

        let num_bytes_written_after_write = self.log_writer.num_bytes_written;

        let command_pos = CommandPos {
            pos: num_bytes_written_before_write,
            len: num_bytes_written_after_write - num_bytes_written_before_write,
        };

        self.index.insert(key, command_pos);

        Ok(())
    }

    /// removes key from kv store
    pub fn remove(&mut self, key: String) -> Result<()> {
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

    /// open the KvStore at a given path, and return the KvStore
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path: PathBuf = path.into();

        fs::create_dir_all(&path)?;

        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path.join(format!("kvs.log")))?;

        let mut index = HashMap::new();

        let log_reader = BufReader::new(log_file.try_clone()?);
        let mut bytes_read = 0;
        for line in log_reader.lines() {
            let line = line?;

            let cmd: Command = serde_json::from_str(&line)?;
            let cmd_len = line.len() as u64;

            match cmd {
                Command::Set { key, value: _ } => index.insert(
                    key,
                    CommandPos {
                        pos: bytes_read,
                        len: cmd_len,
                    },
                ),
                Command::Remove { key } => index.remove(&key),
            };

            bytes_read += cmd_len + 1; // because of the newline separating commands
        }

        Ok(Self {
            log_writer: BufWriterWithPosition::new(log_file.try_clone()?, bytes_read),
            log_reader: BufReader::new(log_file),
            index,
        })
    }
}

/// Whether command worked successfully
pub type Result<T> = std::result::Result<T, failure::Error>;

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
