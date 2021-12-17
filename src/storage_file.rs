use std::io::SeekFrom;

use bytes::{Bytes, BytesMut};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

pub const KEY_LEN: u64 = 128;
pub const VALUE_LEN: u64 = 8;
pub const KEY_VALUE_LEN: u64 = KEY_LEN + VALUE_LEN;
pub const ENTRY_LEN: u64 = KEY_VALUE_LEN /* + 4*/;
pub const FILE_SIZE: u64 = ENTRY_LEN * 15_000_000;

pub struct KeyValue {
    pub key: String,
    pub value: u64,
}

pub struct StorageFile {
    file: File,
}

impl StorageFile {
    pub async fn new(mut file: File) -> Result<StorageFile, tokio::io::Error> {
        Ok(Self { file })
    }
    pub async fn read(&self, pos: u64) -> Result<Option<KeyValue>, tokio::io::Error> {
        let mut file = self.file.try_clone().await?;
        let file_len = file.metadata().await?.len();
        let pos = pos % (file_len / KEY_VALUE_LEN) * KEY_VALUE_LEN;
        file.seek(SeekFrom::Start(pos)).await?;
        let mut buf = vec![0u8; KEY_VALUE_LEN as usize];
        let zero_buf = buf.clone();
        file.read_exact(&mut buf).await?;
        if buf == zero_buf {
            return Ok(None);
        }
        let key = String::from_utf8_lossy(&buf[0..KEY_LEN as usize]).to_string();
        let value = u64::from_ne_bytes(buf[KEY_LEN as usize..].try_into().unwrap());
        Ok(Some(KeyValue { key, value }))
    }
    pub async fn write(&mut self, pos: u64, key_value: KeyValue) -> Result<(), tokio::io::Error> {
        let file_len = self.file.metadata().await?.len();
        let mut pos = pos % (file_len / KEY_VALUE_LEN) * KEY_VALUE_LEN;
        self.file.seek(SeekFrom::Start(pos)).await?;
        let mut buf = vec![0u8; KEY_VALUE_LEN as usize];
        self.file.read_exact(&mut buf).await?;

        let key_bytes = key_value.key.into_bytes();
        buf[0..KEY_LEN as usize].copy_from_slice(&key_bytes);
        let value_bytes = key_value.value.to_ne_bytes();
        buf[KEY_LEN as usize..].copy_from_slice(&value_bytes);
        loop {
            if buf.iter().all(|b| *b == 0) {
                break;
            }
            if &key_bytes == &buf[0..KEY_LEN as usize] {
                break;
            }
            pos = (pos + KEY_VALUE_LEN) % file_len;
            self.file.seek(SeekFrom::Start(pos)).await?;
            self.file.read_exact(&mut buf).await?;
        }
        // self.items_count += 1;
        self.file.seek(SeekFrom::Start(pos)).await?;
        self.file.write_all(&buf).await?;
        Ok(())
    }
}
