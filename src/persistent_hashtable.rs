use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::ffi::OsString;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;

use thiserror::Error;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;

pub const KEY_LEN: u64 = 128;
pub const VALUE_LEN: u64 = 8;
pub const KEY_VALUE_LEN: u64 = KEY_LEN + VALUE_LEN;
pub const ENTRY_LEN: u64 = KEY_VALUE_LEN /*+ 4 */;
pub const FILE_SIZE: u64 = ENTRY_LEN * 2_000_000;

pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: u64,
}

pub struct PersistentHashtable {
    current_file: File,
    table: HashMap<Vec<u8>, u64>,
    current_file_num: usize,
    base_path: PathBuf,
}

#[derive(Debug, Error)]
pub enum ParseHashtableError {
    #[error(transparent)]
    IOError(#[from] tokio::io::Error),
    #[error("Invalid item in database: {0}")]
    InvalidItemError(PathBuf),
    #[error("Got more than {0} items.")]
    TooManyItems(usize),
    #[error("Not enough items, {0} expected, {1} got")]
    NotEnoughItems(usize, usize),
}

#[derive(Debug, Error)]
#[error(transparent)]
pub struct DataStorageOperationError(#[from] tokio::io::Error);

pub fn deserialize_entry(entry_bytes: &[u8]) -> (Vec<u8>, u64) {
    (
        entry_bytes[0..KEY_LEN as usize].to_vec(),
        u64::from_ne_bytes(entry_bytes[KEY_LEN as usize..].try_into().unwrap()),
    )
}
pub fn serialize_entry(key_value: KeyValue) -> Vec<u8> {
    let mut buf = key_value.key;
    buf.extend(key_value.value.to_ne_bytes());
    buf
}
impl PersistentHashtable {
    pub async fn new(base_path: PathBuf) -> Result<PersistentHashtable, ParseHashtableError> {
        if !base_path.exists() {
            tokio::fs::create_dir(&base_path).await?;
        }
        let mut dir_files_iter = tokio::fs::read_dir(&base_path).await?;
        let mut dir_files = Vec::new();
        loop {
            let entry = dir_files_iter.next_entry().await?;
            if let Some(entry) = entry {
                dir_files.push(entry);
            } else {
                break;
            }
        }
        let mut table = HashMap::new();
        let extract_file_num = |s: &OsString| {
            s.to_str()
                .and_then(|s| s.split(".").next())
                .and_then(|num_str| num_str.parse::<usize>().ok())
        };
        dir_files.sort_unstable_by(|f1, f2| {
            let f1_num = if let Some(num) = extract_file_num(&f1.file_name()) {
                num
            } else {
                return Ordering::Less;
            };
            let f2_num = if let Some(num) = extract_file_num(&f2.file_name()) {
                num
            } else {
                return Ordering::Less;
            };
            f1_num.cmp(&f2_num)
        });
        for (num, entry) in dir_files.iter().enumerate() {
            let file_type = entry.file_type().await?;
            if !file_type.is_file() {
                return Err(ParseHashtableError::InvalidItemError(entry.path()));
            }
            let file_num = if let Some(file_num) = extract_file_num(&entry.file_name()) {
                file_num
            } else {
                return Err(ParseHashtableError::InvalidItemError(entry.path()));
            };
            if num != file_num {
                return Err(ParseHashtableError::InvalidItemError(entry.path()));
            }
            let mut file = OpenOptions::new().read(true).open(entry.path()).await?;
            let file_len = file.metadata().await?.len();
            let mut buf = vec![0u8; ENTRY_LEN as usize];
            for i in 0..file_len / KEY_VALUE_LEN {
                file.read_exact(&mut buf).await?;
                let (key, value) = deserialize_entry(&buf);
                table.insert(key, value);
            }
        }

        Ok(Self {
            current_file: OpenOptions::new()
                .create(true)
                .append(true)
                .open(base_path.join(format!("{}.kek", dir_files.len())))
                .await?,
            table,
            current_file_num: dir_files.len(),
            base_path,
        })
    }
    pub async fn get(&self, key: Vec<u8>) -> u64 {
        self.table.get(&key).map(Clone::clone).unwrap_or(0)
    }
    pub async fn set(&mut self, key: Vec<u8>, value: u64) -> Result<(), DataStorageOperationError> {
        let file_len = self.current_file.metadata().await?.len();
        if file_len >= FILE_SIZE {
            self.current_file_num += 1;
            self.current_file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(
                    self.base_path
                        .join(format!("{}.kek", self.current_file_num)),
                )
                .await?;
        }
        let kv = KeyValue { key, value };
        let buf = serialize_entry(kv);
        self.current_file.write_all(&buf).await?;
        self.current_file.flush().await?;
        self.current_file.sync_all().await?;
        Ok(())
    }
}
