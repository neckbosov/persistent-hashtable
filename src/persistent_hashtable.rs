use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::ffi::OsString;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;

use thiserror::Error;
use tokio::fs::OpenOptions;
use tokio::sync::RwLock;

use crate::storage_file::{FILE_SIZE, KeyValue, StorageFile};

const SPLIT_EXPONENT: usize = 2;
const SPLIT_FACTOR: usize = 1usize << SPLIT_EXPONENT;
const SPLIT_MASK: u64 = (SPLIT_FACTOR as u64) - 1;

pub struct PersistentHashtable {
    files: Vec<RwLock<StorageFile>>,
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

impl PersistentHashtable {
    pub async fn new(base_path: PathBuf) -> Result<PersistentHashtable, ParseHashtableError> {
        if !base_path.exists() {
            tokio::fs::create_dir(&base_path).await?;
        }
        let mut dir_files_iter = tokio::fs::read_dir(&base_path).await?;
        let mut dir_files = Vec::new();
        let expected_files = SPLIT_FACTOR;
        for _ in 0..expected_files {
            let entry = dir_files_iter.next_entry().await?;
            if let Some(entry) = entry {
                dir_files.push(entry);
            } else {
                break;
            }
        }

        if let Some(_) = dir_files_iter.next_entry().await? {
            return Err(ParseHashtableError::TooManyItems(expected_files));
        }

        let mut files = Vec::new();
        if dir_files.is_empty() {
            for num in 0..expected_files {
                let file_name = format!("{}.kek", num);
                let file = OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .custom_flags(libc::O_DIRECT)
                    .open(base_path.join(file_name))
                    .await?;
                file.set_len(FILE_SIZE).await?;
                files.push(RwLock::new(StorageFile::new(file).await?));
            }
        } else {
            if dir_files.len() < expected_files {
                return Err(ParseHashtableError::NotEnoughItems(
                    expected_files,
                    dir_files.len(),
                ));
            }
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
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .custom_flags(libc::O_DIRECT)
                    .open(entry.path())
                    .await?;
                // let mut items_count = 0;
                // let mut buf = vec![0u8; ENTRY_LEN as usize];
                // for i in 0..FILE_SIZE / KEY_VALUE_LEN {
                //     file.read_exact(&mut buf).await?;
                //     if !buf.iter().all(|b| *b == 0) {
                //         items_count += 1;
                //     }
                // }
                // file.seek(SeekFrom::Start(0)).await?;
                files.push(RwLock::new(StorageFile::new(file).await?));
            }
        }

        Ok(Self { files })
    }
    pub async fn get(&self, key: String) -> Result<u64, DataStorageOperationError> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let pos = hasher.finish();
        let file_num = (pos & SPLIT_MASK) as usize;
        let file_guard = self.files[file_num].read().await;
        file_guard
            .read(pos >> SPLIT_EXPONENT as u64)
            .await
            .map(|kv| kv.map(|kv| kv.value).unwrap_or(0))
            .map_err(|err| err.into())
    }
    pub async fn set(&self, key: String, value: u64) -> Result<(), DataStorageOperationError> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let pos = hasher.finish();
        let file_num = (pos & SPLIT_MASK) as usize;
        let mut file_guard = self.files[file_num].write().await;
        file_guard
            .write(pos >> SPLIT_EXPONENT as u64, KeyValue { key, value })
            .await
            .map_err(|err| err.into())
    }
}
