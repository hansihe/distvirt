use clap::Parser;
use distvirt_client_protocol::DistvirtClientServer;
use distvirt_orchestrator::adapter::timer::TimerConfig;
use distvirt_orchestrator::config::OrchestratorConfig;
use distvirt_orchestrator::grpc::DistvirtClientService;
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

    let worker_secret = cli
        .worker_secret
        .or(config.workers.secret)
        .expect("worker secret must be set via --worker-secret or workers.secret in config");
    let client_secret = cli.client_secret.or(config.grpc.secret);

    let timer_config = TimerConfig {
        retry_backoff: std::time::Duration::from_secs(5),
        launch_timeout: std::time::Duration::from_secs(60),
        suspend_timeout: std::time::Duration::from_secs(60),
        idle_timeout: std::time::Duration::from_secs(30),
    };
    let (handle, log_bus, event_bus, id_registry_map, shell_handle) =
        distvirt_orchestrator::shell::r#async::spawn(worker_secret, timer_config, config.tunnels.encrypted);

    // Start gRPC server.
    let grpc_addr = config.grpc.listen.parse()?;
    let grpc_handle = handle.clone();
    tokio::spawn(async move {
        let svc = DistvirtClientService::new(grpc_handle, log_bus, event_bus, id_registry_map);
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
    let worker_handle = handle.clone();
    let worker_listener = tokio::net::TcpListener::bind(&config.workers.listen).await?;
    log::info!("Worker listener on {}", config.workers.listen);
    tokio::spawn(async move {
        loop {
            match worker_listener.accept().await {
                Ok((socket, addr)) => {
                    log::info!("worker connection from {}", addr);
                    let handle = worker_handle.clone();
                    tokio::spawn(async move {
                        match OrchestratorConnection::connect(socket).await {
                            Ok(conn) => handle.worker_connection(conn),
                            Err(e) => log::error!("worker connection setup failed: {}", e),
                        }
                    });
                }
                Err(e) => log::error!("worker accept error: {}", e),
            }
        }
    });

    // Wait for shell to complete.
    shell_handle.await?;

    Ok(())
}
