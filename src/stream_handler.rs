use std::sync::Arc;

use libc::mode_t;
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, Mutex};

use crate::kv::*;
use crate::persistent_hashtable::{DataStorageOperationError, PersistentHashtable};
use crate::rpc::{GET_RESPONSE, parse_request, ParseRequestError, PUT_RESPONSE, Request};

#[derive(Debug, thiserror::Error)]
pub enum StreamHandleError {
    #[error(transparent)]
    StreamIOError(#[from] std::io::Error),
    #[error(transparent)]
    ParseError(#[from] ParseRequestError),
}

pub struct StreamHandler {
    stream_reader: OwnedReadHalf,
    stream_writer: Arc<Mutex<OwnedWriteHalf>>,
    hashtable: Arc<PersistentHashtable>,
    shutdown: broadcast::Receiver<()>,
    inner_sender: broadcast::Sender<()>,
    inner_receiver: broadcast::Receiver<()>,
    // db_error_sender: mpsc::UnboundedSender<DataStorageOperationError>,
    // db_error_receiver: mpsc::UnboundedReceiver<DataStorageOperationError>,
}

impl StreamHandler {
    pub fn new(
        stream: TcpStream,
        hashtable: Arc<PersistentHashtable>,
        shutdown: broadcast::Receiver<()>,
    ) -> Self {
        let (inner_sender, inner_receiver) = broadcast::channel(1);
        // let (db_error_sender, db_error_receiver) = mpsc::unbounded_channel();
        let (read_half, write_half) = stream.into_split();
        Self {
            stream_reader: read_half,
            stream_writer: Arc::new(Mutex::new(write_half)),
            hashtable,
            shutdown,
            inner_sender,
            inner_receiver,
            // db_error_sender,
            // db_error_receiver,
        }
    }
    pub async fn run(&mut self) -> Result<(), StreamHandleError> {
        loop {
            let request_type: u8 = tokio::select! {
                request_type = self.stream_reader.read_u8() => {
                    request_type?
                }
                _ = self.shutdown.recv() => {
                    self.inner_sender.send(());
                    return Ok(());
                }
            };
            let request_len: u32 = tokio::select! {
                request_len = self.stream_reader.read_u32() => {
                    request_len?
                }
                _ = self.shutdown.recv() => {
                    self.inner_sender.send(());
                    return Ok(());
                }
            };
            let mut buf = vec![0u8; request_len as usize];
            tokio::select! {
                read_result = self.stream_reader.read_exact(&mut buf) => {
                    read_result?;
                }
                _ = self.shutdown.recv() => {
                    self.inner_sender.send(());
                    return Ok(());
                }
            }
            let request = parse_request(request_type, buf)?;
            let shutdown = self.inner_sender.subscribe();
            let output_stream = Arc::clone(&self.stream_writer);
            let hashtable = Arc::clone(&self.hashtable);
            tokio::spawn(async move {
                handle_request(request, hashtable, shutdown, output_stream);
            });
        }
    }
}

async fn handle_request(
    request: Request,
    hashtable: Arc<PersistentHashtable>,
    shutdown: broadcast::Receiver<()>,
    output_stream: Arc<Mutex<OwnedWriteHalf>>,
) {
    match request {
        Request::Get(get_request) => {
            let value = hashtable.get(get_request.key).await.unwrap_or(0);
            let response = TGetResponse {
                request_id: get_request.request_id,
                offset: value,
            };
            let buf = response.encode_to_vec();
            let mut stream = output_stream.lock().await;
            (*stream).write_u8(GET_RESPONSE).await;
            (*stream).write_u32(buf.len() as u32).await;
            (*stream).write_all(&buf).await;
        }
        Request::Put(put_request) => {
            hashtable.set(put_request.key, put_request.offset).await;
            let response = TPutResponse {
                request_id: put_request.request_id,
            };
            let buf = response.encode_to_vec();
            let mut stream = output_stream.lock().await;
            (*stream).write_u8(PUT_RESPONSE).await;
            (*stream).write_u32(buf.len() as u32).await;
            (*stream).write_all(&buf).await;
        }
    }
}
