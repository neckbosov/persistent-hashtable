use std::io::SeekFrom;

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

pub const KEY_LEN: u64 = 128;
pub const VALUE_LEN: u64 = 8;
pub const KEY_VALUE_LEN: u64 = KEY_LEN + VALUE_LEN;
pub const ENTRY_LEN: u64 = KEY_VALUE_LEN /*+ 4 */;
pub const FILE_SIZE: u64 = ENTRY_LEN * 15_000_000;

pub struct KeyValue {
    pub key: String,
    pub value: u64,
}

// fn bytes_sum(bytes: &[u8]) -> u32 {
//     bytes.iter().fold(0u32, |b, x| b + *x as u32)
// }

enum KeyEntryState {
    Free,
    // Corrupted,
    Different,
    Same(u64),
}

fn get_entry_state(key: &[u8], entry_bytes: &[u8]) -> KeyEntryState {
    // let bytes_crc = u32::from_ne_bytes(entry_bytes[KEY_VALUE_LEN as usize..].try_into().unwrap());
    if entry_bytes.iter().all(|b| *b == 0) {
        KeyEntryState::Free
    }
    // else if bytes_sum(&entry_bytes[0..KEY_VALUE_LEN as usize]).wrapping_add(bytes_crc) != 0 {
    //     KeyEntryState::Corrupted
    // }
    else if key == &entry_bytes[0..KEY_LEN as usize] {
        let value = u64::from_ne_bytes(
            entry_bytes[KEY_LEN as usize..KEY_VALUE_LEN as usize]
                .try_into()
                .unwrap(),
        );
        KeyEntryState::Same(value)
    } else {
        KeyEntryState::Different
    }
}

pub struct StorageFile {
    file: File,
}

impl StorageFile {
    pub async fn new(file: File) -> Result<StorageFile, tokio::io::Error> {
        Ok(Self { file })
    }
    pub async fn read(&self, pos: u64, key: String) -> Result<Option<KeyValue>, tokio::io::Error> {
        let mut file = self.file.try_clone().await?;
        let file_len = file.metadata().await?.len();
        let pos = pos % (file_len / ENTRY_LEN) * ENTRY_LEN;
        file.seek(SeekFrom::Start(pos)).await?;
        let mut buf = vec![0u8; ENTRY_LEN as usize];
        file.read_exact(&mut buf).await?;
        let key_bytes = key.clone().into_bytes();
        let mut pos = pos;
        let value = loop {
            match get_entry_state(key_bytes.as_slice(), buf.as_slice()) {
                KeyEntryState::Free => {
                    break None;
                }
                // KeyEntryState::Corrupted => {
                //     file.seek(SeekFrom::Start(pos)).await?;
                //     file.write_all(&zero_buf).await?;
                //     pos = (pos + ENTRY_LEN) % file_len;
                //     file.seek(SeekFrom::Start(pos)).await?;
                //     file.read_exact(&mut buf).await?;
                // }
                KeyEntryState::Different => {
                    pos = (pos + ENTRY_LEN) % file_len;
                    file.seek(SeekFrom::Start(pos)).await?;
                    file.read_exact(&mut buf).await?;
                }
                KeyEntryState::Same(value) => {
                    break Some(value);
                }
            }
        };
        Ok(value.map(|value| KeyValue { key, value }))
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
            match get_entry_state(key_bytes.as_slice(), buf.as_slice()) {
                KeyEntryState::Free => {
                    break;
                }
                // KeyEntryState::Corrupted => {
                //     file.seek(SeekFrom::Start(pos)).await?;
                //     file.write_all(&zero_buf).await?;
                //     pos = (pos + ENTRY_LEN) % file_len;
                //     file.seek(SeekFrom::Start(pos)).await?;
                //     file.read_exact(&mut buf).await?;
                // }
                KeyEntryState::Different => {
                    pos = (pos + ENTRY_LEN) % file_len;
                    self.file.seek(SeekFrom::Start(pos)).await?;
                    self.file.read_exact(&mut buf).await?;
                }
                KeyEntryState::Same(_) => {
                    break;
                }
            }
        }
        // self.items_count += 1;
        self.file.seek(SeekFrom::Start(pos)).await?;
        self.file.write_all(&buf).await?;
        Ok(())
    }
}
