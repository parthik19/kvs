use crate::KvsEngine;
use crate::Result;
use failure::format_err;
use std::path::PathBuf;

// sled storage stuff starts here
/// thin wrapper around the sled db
pub struct SledKvsEngine {
    inner: sled::Db,
}

impl SledKvsEngine {
    /// create the sled db at some specified path
    pub fn open(path: impl Into<PathBuf>) -> Result<SledKvsEngine> {
        Ok(Self {
            inner: sled::open(path.into().join("sled_db.log"))?,
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

        if result.is_some() {
            Ok(())
        } else {
            Err(format_err!("Removing non existent key"))
        }
    }
}
