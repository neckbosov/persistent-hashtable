use std::io::Cursor;
use std::sync::Arc;

use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, Mutex, RwLock, watch};

use crate::kv::*;
use crate::persistent_hashtable::PersistentHashtable;
use crate::rpc::{GET_RESPONSE, parse_request, ParseRequestError, PUT_RESPONSE, Request};

#[derive(Debug, thiserror::Error)]
pub enum StreamHandleError {
    #[error(transparent)]
    StreamIOError(#[from] std::io::Error),
    #[error(transparent)]
    ParseError(#[from] ParseRequestError),
}

pub struct StreamHandler {
    stream: TcpStream,
    hashtable: Arc<RwLock<PersistentHashtable>>,
    shutdown: watch::Receiver<()>,
    inner_sender: watch::Sender<()>,
    inner_receiver: watch::Receiver<()>,
    // db_error_sender: mpsc::UnboundedSender<DataStorageOperationError>,
    // db_error_receiver: mpsc::UnboundedReceiver<DataStorageOperationError>
}

impl StreamHandler {
    pub fn new(
        mut stream: TcpStream,
        hashtable: Arc<RwLock<PersistentHashtable>>,
        shutdown: watch::Receiver<()>,
    ) -> Self {
        let (inner_sender, inner_receiver) = watch::channel(());
        stream.set_nodelay(true);
        // let (db_error_sender, db_error_receiver) = mpsc::unbounded_channel();
        Self {
            stream,
            hashtable,
            shutdown,
            inner_sender,
            inner_receiver,
            // db_error_sender,
            // db_error_receiver,
        }
    }
    pub async fn run(mut self) -> Result<(), StreamHandleError> {
        'oop: loop {
            // println!("iter!");
            let mut header_buf = vec![0u8; 5];
            let mut header_read = 0;
            while header_read < header_buf.len() {
                tokio::select! {
                    res = self.stream.read(&mut header_buf[header_read..]) => {
                        let n = res?;
                        if n == 0 {
                            break 'oop;
                        }
                        header_read += n;
                    }
                    _ = self.shutdown.changed() => {
                        self.inner_sender.send(()).unwrap();
                        return Ok(());
                    }
                };
            }

            let request_type = header_buf[0];
            // println!("read type {}", request_type);
            let request_len: u32 = u32::from_le_bytes(header_buf[1..].try_into().unwrap());
            // println!("read len {}", request_len);
            let mut buf = vec![0u8; request_len as usize];
            tokio::select! {
                read_result = self.stream.read_exact(&mut buf) => {
                    read_result?;
                }
                _ = self.shutdown.changed() => {
                    self.inner_sender.send(()).unwrap();
                    return Ok(());
                }
            }
            let request = parse_request(request_type, buf)?;
            let hashtable = Arc::clone(&self.hashtable);
            // handle_request(request, hashtable,  &mut self.stream).await;
            tokio::select! {
                _ = handle_request(request, hashtable,  &mut self.stream) => {
                }
                _ = shutdown.changed() => {}
            }
        }
        Ok(())
    }
}

async fn handle_request(
    request: Request,
    hashtable: Arc<RwLock<PersistentHashtable>>,
    output_stream: &mut TcpStream,
) {
    match request {
        Request::Get(get_request) => {
            // println!("Get request {}", &get_request.key);

            let value = {
                let lock = hashtable.read().await;
                lock.get(get_request.key.into_bytes()).await
            };
            let response = TGetResponse {
                request_id: get_request.request_id,
                offset: value,
            };
            let buf = response.encode_to_vec();
            let mut stream = output_stream;
            (*stream).write_u8(GET_RESPONSE).await.unwrap();
            (*stream).write_u32(buf.len() as u32).await.unwrap();
            (*stream).write_all(&buf).await.unwrap();
            // (*stream).flush().await.unwrap();
        }
        Request::Put(put_request) => {
            // println!("Put request {} {}", &put_request.key, &put_request.offset);
            let mut lock = hashtable.write().await;
            lock.set(put_request.key.into_bytes(), put_request.offset)
                .await
                .unwrap();
            // println!("kek");
            let response = TPutResponse {
                request_id: put_request.request_id,
            };
            let buf = response.encode_to_vec();
            let mut stream = output_stream;
            let mut res_buf = vec![1u8; buf.len() + 5];
            let mut c = Cursor::new(&mut res_buf);
            c.write_u8(PUT_RESPONSE).await.unwrap();
            c.write_u32(buf.len() as u32).await.unwrap();
            c.write_all(&buf).await.unwrap();
            stream.write_all(&res_buf).await.unwrap();
            // (*stream).flush().await.unwrap();
            // println!("Put finished");
        }
    }
}
