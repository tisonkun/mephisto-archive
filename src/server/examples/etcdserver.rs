use mephisto_server::etcdserver::{proto::etcdserverpb::kv_server::KvServer, EtcdServer};
use tonic::transport::Server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = "127.0.0.1:2379".parse()?;
    let greeter = EtcdServer::default();

    Server::builder()
        .add_service(KvServer::new(greeter))
        .serve(addr)
        .await?;

    Ok(())
}
