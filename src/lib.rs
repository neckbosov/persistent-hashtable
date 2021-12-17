pub mod persistent_hashtable;
pub mod rpc;
pub mod storage_file;
pub mod stream_handler;

pub mod kv {
    include!(concat!(env!("OUT_DIR"), "/n_proto.rs"));
}
