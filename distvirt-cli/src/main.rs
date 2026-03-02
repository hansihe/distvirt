use std::path::PathBuf;

use clap::{Parser, Subcommand};

mod client;
mod commands;
mod config;
mod connection;
mod format;
mod tun;

use commands::{LegacyCommands, OutputFormat};

#[derive(Parser)]
#[command(name = "dv", about = "distvirt CLI — lightweight VM-based container orchestration")]
struct Cli {
    /// Server address (host:port or URL)
    #[arg(long, global = true)]
    server: Option<String>,

    /// API token for authentication
    #[arg(long, global = true)]
    token: Option<String>,

    /// Named context to use from credentials file
    #[arg(long, global = true)]
    context: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Apply a namespace spec (create or update)
    Up {
        /// Namespace ID
        namespace_id: String,
        /// Path to the compose file
        #[arg(short, long, default_value = "docker-compose.yml")]
        file: PathBuf,
    },
    /// Delete a namespace
    Down {
        /// Namespace ID
        namespace_id: String,
    },
    /// Show live namespace status
    Status {
        /// Target: "namespace" or "namespace/workload"
        target: String,
    },
    /// Stream logs from a namespace
    Logs {
        /// Namespace ID
        namespace_id: String,
        /// Filter to a specific workload
        #[arg(long)]
        workload: Option<String>,
        /// Follow log output
        #[arg(short, long)]
        follow: bool,
    },
    /// Stream events from a namespace
    Events {
        /// Namespace ID
        namespace_id: String,
        /// Follow event stream
        #[arg(short, long)]
        follow: bool,
    },
    /// Splice a workload to a local worker
    Splice {
        /// Namespace ID
        namespace_id: String,
        /// Workload ID
        workload_id: String,
        /// Local worker ID
        worker_id: String,
    },
    /// Clone a namespace
    Clone {
        /// Source namespace ID
        source: String,
        /// Target namespace ID
        target: String,
    },
    /// Get resources
    Get {
        /// Resource type (namespaces, workers, pods)
        resource: String,
        /// Resource name (optional)
        name: Option<String>,
        /// Namespace for scoped resources
        #[arg(short, long)]
        namespace: Option<String>,
        /// Output format
        #[arg(short, long, value_enum, default_value_t = OutputFormat::Text)]
        output: OutputFormat,
    },
    /// Describe a resource in detail
    Describe {
        /// Resource type
        resource: String,
        /// Resource name
        name: String,
        /// Output format
        #[arg(short, long, value_enum, default_value_t = OutputFormat::Text)]
        output: OutputFormat,
    },
    /// Create a resource
    Create {
        /// Resource type
        resource: String,
    },
    /// Delete a resource
    Delete {
        /// Resource type
        resource: String,
        /// Resource name
        name: String,
    },
    /// Log in and save credentials
    Login {
        /// Server address
        #[arg(long)]
        server: Option<String>,
        /// API token
        #[arg(long)]
        token: Option<String>,
    },
    /// Manage named contexts
    Context {
        #[command(subcommand)]
        command: Option<ContextCommands>,
    },
    /// Connect to a namespace network via WireGuard tunnel
    Connect {
        /// Namespace ID
        namespace_id: String,
        /// Print wg-quick config instead of establishing tunnel
        #[arg(long)]
        config: bool,
    },
    /// Disconnect from a namespace network
    Disconnect {
        /// Namespace ID
        namespace_id: String,
    },
    /// Legacy in-process commands (compose-up, run-image)
    Legacy {
        #[command(subcommand)]
        command: LegacyCommands,
    },
}

#[derive(Subcommand)]
enum ContextCommands {
    /// Switch to a named context
    Use {
        /// Context name
        name: String,
    },
    /// List all contexts
    List,
    /// Delete a context
    Delete {
        /// Context name
        name: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .parse_default_env()
        .init();

    let cli = Cli::parse();

    match cli.command {
        // Auth commands — no gRPC needed
        Commands::Login { server, token } => {
            commands::auth::login(server.as_deref(), token.as_deref())?;
        }
        Commands::Context { command } => match command {
            None => commands::auth::context_show()?,
            Some(ContextCommands::Use { name }) => commands::auth::context_use(&name)?,
            Some(ContextCommands::List) => commands::auth::context_list()?,
            Some(ContextCommands::Delete { name }) => commands::auth::context_delete(&name)?,
        },

        // Legacy commands run in-process, no gRPC needed
        Commands::Legacy { command } => {
            commands::legacy::run(command).await?;
        }

        // All other commands connect to the orchestrator
        cmd => {
            let params = connection::resolve(
                cli.server.as_deref(),
                cli.token.as_deref(),
                cli.context.as_deref(),
            )?;
            let client = client::connect(&params).await?;

            match cmd {
                Commands::Up { namespace_id, file } => {
                    commands::namespace::up(client, &namespace_id, &file).await?;
                }
                Commands::Down { namespace_id } => {
                    commands::namespace::down(client, &namespace_id).await?;
                }
                Commands::Status { target } => {
                    commands::namespace::status(client, &target).await?;
                }
                Commands::Logs {
                    namespace_id,
                    workload,
                    follow,
                } => {
                    commands::streaming::logs(
                        client,
                        &namespace_id,
                        workload.as_deref(),
                        follow,
                    )
                    .await?;
                }
                Commands::Events {
                    namespace_id,
                    follow,
                } => {
                    commands::streaming::events(client, &namespace_id, follow).await?;
                }
                Commands::Splice {
                    namespace_id,
                    workload_id,
                    worker_id,
                } => {
                    commands::splice::splice(client, &namespace_id, &workload_id, &worker_id)
                        .await?;
                }
                Commands::Clone { source, target } => {
                    commands::namespace::clone_namespace(client, &source, &target).await?;
                }
                Commands::Get {
                    resource,
                    name: _,
                    namespace,
                    output,
                } => {
                    commands::resource::get(client, &resource, namespace.as_deref(), &output)
                        .await?;
                }
                Commands::Describe {
                    resource,
                    name,
                    output,
                } => {
                    commands::resource::describe(client, &resource, &name, &output).await?;
                }
                Commands::Create { resource } => {
                    commands::resource::create(client, &resource).await?;
                }
                Commands::Delete { resource, name } => {
                    commands::resource::delete(client, &resource, &name).await?;
                }
                Commands::Connect {
                    namespace_id,
                    config,
                } => {
                    commands::connect::connect(client, &namespace_id, config).await?;
                }
                Commands::Disconnect { namespace_id } => {
                    commands::connect::disconnect(client, &namespace_id).await?;
                }
                Commands::Login { .. }
                | Commands::Context { .. }
                | Commands::Legacy { .. } => unreachable!(),
            }
        }
    }

    Ok(())
}
