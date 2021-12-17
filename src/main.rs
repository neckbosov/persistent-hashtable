use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::io::Cursor;
use std::path::PathBuf;

use prost::Message;
use structopt::StructOpt;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::broadcast;

use persistent_storage::kv::{TGetRequest, TPutRequest};
use persistent_storage::persistent_hashtable::{ParseHashtableError, PersistentHashtable};

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(long)]
    /// Port for listening requests
    port: u16,
    #[structopt(long, parse(from_os_str), default_value = "./storage")]
    /// Path to store and read storage data
    path: PathBuf,
}

#[tokio::main]
async fn main() {
    let opts: Opt = Opt::from_args();
    let hashtable = PersistentHashtable::new(opts.path).await.unwrap();
}
