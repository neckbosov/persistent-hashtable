use std::path::PathBuf;
use std::sync::Arc;

use structopt::StructOpt;
use tokio::net::TcpListener;
use tokio::sync::watch;

use persistent_storage::persistent_hashtable::PersistentHashtable;
use persistent_storage::stream_handler::StreamHandler;

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
    let (shutdown_sender, shutdown_receiver) = watch::channel(());
    let receiver = shutdown_receiver.clone();
    let mut server = tokio::spawn(async move {
        let hashtable = Arc::new(hashtable);
        let listener = TcpListener::bind(format!("localhost:{}", opts.port))
            .await
            .unwrap();
        loop {
            let mut shutdown_receiver = receiver.clone();
            let (socket, _) = tokio::select! {
                res = listener.accept() => {
                    res.unwrap()
                }
                _ = shutdown_receiver.changed() => {
                    break;
                }
            };
            let mut handler = StreamHandler::new(socket, Arc::clone(&hashtable), shutdown_receiver);
            tokio::spawn(async move {
                handler.run().await.unwrap();
            });
        }
    });
    match tokio::signal::ctrl_c().await {
        Ok(_) => {
            shutdown_sender.send(());
        }
        Err(_) => {}
    }
    server.await;
}
