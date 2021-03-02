use crate::{Command, CommandPos, KvsEngine, Result, COMPACTION_THRESHOLD};
use failure::format_err;
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, Write};
use std::path::PathBuf;

/// holds the key value pairings
pub struct KvStore {
    log_writer: BufWriterWithPosition<File>,
    log_reader: BufReader<File>,
    index: HashMap<String, CommandPos>,
    num_unnecessary_entries: usize,
    path: PathBuf, // the path it was initially opened with
}

impl KvStore {
    /// create a kv store at a certain path (kvs.log will be created here)
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path: PathBuf = path.into();

        fs::create_dir_all(&path)?;

        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path.join("kvs.log"))?;

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
                writeln!(&mut new_log_file)?;
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
            value,
        };

        let num_bytes_written_before_write = self.log_writer.num_bytes_written;

        serde_json::to_writer(&mut self.log_writer, &command)?;
        self.log_writer.write_all(b"\n")?;

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
            self.log_writer.write_all(b"\n")?;

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
