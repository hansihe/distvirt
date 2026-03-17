use clap::Parser;
use distvirt_client_protocol::DistvirtClientServer;
use distvirt_orchestrator::config::OrchestratorConfig;
use distvirt_orchestrator::grpc::DistvirtClientService;
use distvirt_orchestrator::shell::OrchestratorShell;
use distvirt_worker_protocol::OrchestratorConnection;

#[derive(Parser)]
#[command(name = "distvirt-orchestrator")]
struct Cli {
    /// Path to the TOML configuration file
    #[arg(short, long)]
    config: String,

    /// Shared secret for worker authentication (overrides config file)
    #[arg(long)]
    worker_secret: Option<String>,

    /// Shared secret for client API authentication (overrides config file)
    #[arg(long)]
    client_secret: Option<String>,
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
    let worker_secret = cli
        .worker_secret
        .or(config.workers.secret)
        .expect("worker secret must be set via --worker-secret or workers.secret in config");
    let client_secret = cli.client_secret.or(config.grpc.secret);
    let mut shell = OrchestratorShell::new(
        config.wireguard.listen_port,
        config.tunnels.encrypted,
        pool_configs,
        worker_secret,
    );
    let handle = shell.handle();

    // Start gRPC server.
    let grpc_addr = config.grpc.listen.parse()?;
    let grpc_handle = handle.clone();
    tokio::spawn(async move {
        let svc = DistvirtClientService::new(grpc_handle);
        log::info!("gRPC server listening on {}", grpc_addr);
        let mut server = tonic::transport::Server::builder();
        let result = if let Some(secret) = client_secret {
            server
                .add_service(DistvirtClientServer::with_interceptor(svc, move |req| {
                    distvirt_orchestrator::grpc::check_client_auth(req, &secret)
                }))
                .serve(grpc_addr)
                .await
        } else {
            server
                .add_service(DistvirtClientServer::new(svc))
                .serve(grpc_addr)
                .await
        };
        if let Err(e) = result {
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
