use std::path::PathBuf;

use structopt::StructOpt;

use persistent_storage::persistent_hashtable::PersistentHashtable;

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
    let _hashtable = PersistentHashtable::new(opts.path).await.unwrap();
}
