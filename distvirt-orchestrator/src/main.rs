use clap::Parser;
use distvirt_orchestrator::config::OrchestratorConfig;
use distvirt_orchestrator::grpc::DistvirtClientService;
use distvirt_orchestrator::shell::OrchestratorShell;
use distvirt_client_protocol::DistvirtClientServer;
use distvirt_worker_protocol::OrchestratorConnection;

#[derive(Parser)]
#[command(name = "distvirt-orchestrator")]
struct Cli {
    /// Path to the TOML configuration file
    #[arg(short, long)]
    config: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let cli = Cli::parse();
    let config_str = std::fs::read_to_string(&cli.config)?;
    let config: OrchestratorConfig = toml::from_str(&config_str)?;

    let pool_configs: Vec<distvirt_worker_protocol::PoolInfo> = config
        .pools
        .iter()
        .map(|pc| distvirt_worker_protocol::PoolInfo {
            pool_id: distvirt_worker_protocol::PoolId::from(pc.pool_id.as_str()),
            path: pc.path.clone(),
            capacity_bytes: 0,
            available_bytes: 0,
        })
        .collect();
    let mut shell = OrchestratorShell::new(config.wireguard.listen_port, config.tunnels.encrypted, pool_configs);
    let handle = shell.handle();

    // Start gRPC server.
    let grpc_addr = config.grpc.listen.parse()?;
    let grpc_handle = handle.clone();
    tokio::spawn(async move {
        let svc = DistvirtClientService::new(grpc_handle);
        log::info!("gRPC server listening on {}", grpc_addr);
        if let Err(e) = tonic::transport::Server::builder()
            .add_service(DistvirtClientServer::new(svc))
            .serve(grpc_addr)
            .await
        {
            log::error!("gRPC server error: {}", e);
        }
    });

    // Start worker TCP listener.
    let worker_listener = tokio::net::TcpListener::bind(&config.workers.listen).await?;
    log::info!("Worker listener on {}", config.workers.listen);
    tokio::spawn(async move {
        loop {
            match worker_listener.accept().await {
                Ok((socket, addr)) => {
                    log::info!("worker connection from {}", addr);
                    let handle = handle.clone();
                    tokio::spawn(async move {
                        match OrchestratorConnection::connect(socket).await {
                            Ok(conn) => handle.submit_worker_connection(conn),
                            Err(e) => log::error!("worker connection setup failed: {}", e),
                        }
                    });
                }
                Err(e) => log::error!("worker accept error: {}", e),
            }
        }
    });

    // Run shell message loop (blocks until shutdown).
    shell.run().await
}
