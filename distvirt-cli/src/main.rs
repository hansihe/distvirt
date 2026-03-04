use std::path::PathBuf;

use clap::{Parser, Subcommand};

mod client;
mod commands;
mod config;
mod connection;
mod format;
mod platform;

use commands::{LegacyCommands, OutputFormat};

const GROUPED_HELP: &str = "\x1b[1;4mTask commands:\x1b[0m
  up            Apply a namespace spec (create or update)
  down          Delete a namespace
  status        Show namespace/workload status
  logs          Stream logs from a namespace
  events        Stream events from a namespace
  deactivate    Hint the orchestrator to deactivate a workload
  connect       Connect to a namespace network via WireGuard
  disconnect    Disconnect from a namespace network
  clone         Clone a namespace
  splice        Splice a workload to a local worker

\x1b[1;4mResource commands:\x1b[0m
  get           List resources
  describe      Describe a resource in detail
  create        Create a resource
  delete        Delete a resource

\x1b[1;4mAuth & config:\x1b[0m
  login         Log in and save credentials
  context       Manage named contexts

\x1b[1;4mOther:\x1b[0m
  legacy        Legacy in-process commands";

#[derive(Parser)]
#[command(
    name = "dv",
    about = "distvirt CLI — lightweight VM-based container orchestration",
    after_help = GROUPED_HELP,
    disable_help_subcommand = true,
)]
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
    #[command(flatten)]
    Task(TaskCommands),
    #[command(flatten)]
    Resource(ResourceCommands),
    #[command(flatten)]
    Auth(AuthCommands),
    #[command(flatten)]
    Other(OtherCommands),
}

/// Layer 1 — task-oriented commands
#[derive(Subcommand)]
enum TaskCommands {
    /// Apply a namespace spec (create or update)
    #[command(hide = true)]
    Up {
        /// Namespace ID
        namespace_id: String,
        /// Path to the compose file
        #[arg(short, long, default_value = "docker-compose.yml")]
        file: PathBuf,
    },
    /// Delete a namespace
    #[command(hide = true)]
    Down {
        /// Namespace ID
        namespace_id: String,
    },
    /// Show live namespace status
    #[command(hide = true)]
    Status {
        /// Target: "namespace" or "namespace/workload"
        target: String,
    },
    /// Stream logs from a namespace
    #[command(hide = true)]
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
    #[command(hide = true)]
    Events {
        /// Namespace ID
        namespace_id: String,
        /// Filter to specific workloads (can be repeated)
        #[arg(long)]
        workload: Vec<String>,
        /// Filter to specific services (can be repeated)
        #[arg(long)]
        service: Vec<String>,
        /// Follow event stream
        #[arg(short, long)]
        follow: bool,
    },
    /// Hint the orchestrator to deactivate a workload immediately
    #[command(hide = true)]
    Deactivate {
        /// Target: "namespace/workload"
        target: String,
    },
    /// Connect to a namespace network via WireGuard tunnel
    #[command(hide = true)]
    Connect {
        /// Namespace ID
        namespace_id: String,
        /// Print wg-quick config instead of establishing tunnel
        #[arg(long)]
        config: bool,
    },
    /// Disconnect from a namespace network
    #[command(hide = true)]
    Disconnect {
        /// Namespace ID
        namespace_id: String,
    },
    /// Clone a namespace
    #[command(hide = true)]
    Clone {
        /// Source namespace ID
        source: String,
        /// Target namespace ID
        target: String,
    },
    /// Splice a workload to a local worker
    #[command(hide = true)]
    Splice {
        /// Namespace ID
        namespace_id: String,
        /// Workload ID
        workload_id: String,
        /// Local worker ID
        worker_id: String,
    },
}

/// Layer 2 — uniform resource commands
#[derive(Subcommand)]
enum ResourceCommands {
    /// Get resources
    #[command(hide = true)]
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
    #[command(hide = true)]
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
    #[command(hide = true)]
    Create {
        /// Resource type
        resource: String,
    },
    /// Delete a resource
    #[command(hide = true)]
    Delete {
        /// Resource type
        resource: String,
        /// Resource name
        name: String,
    },
}

/// Auth & config commands
#[derive(Subcommand)]
enum AuthCommands {
    /// Log in and save credentials
    #[command(hide = true)]
    Login {
        /// Server address
        #[arg(long)]
        server: Option<String>,
        /// API token
        #[arg(long)]
        token: Option<String>,
    },
    /// Manage named contexts
    #[command(hide = true)]
    Context {
        #[command(subcommand)]
        command: Option<ContextCommands>,
    },
}

/// Other commands
#[derive(Subcommand)]
enum OtherCommands {
    /// Legacy in-process commands (compose-up, run-image)
    #[command(hide = true)]
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
        Commands::Auth(AuthCommands::Login { server, token }) => {
            commands::auth::login(server.as_deref(), token.as_deref())?;
        }
        Commands::Auth(AuthCommands::Context { command }) => match command {
            None => commands::auth::context_show()?,
            Some(ContextCommands::Use { name }) => commands::auth::context_use(&name)?,
            Some(ContextCommands::List) => commands::auth::context_list()?,
            Some(ContextCommands::Delete { name }) => commands::auth::context_delete(&name)?,
        },

        // Legacy commands run in-process, no gRPC needed
        Commands::Other(OtherCommands::Legacy { command }) => {
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
                Commands::Task(TaskCommands::Up { namespace_id, file }) => {
                    commands::namespace::up(client, &namespace_id, &file).await?;
                }
                Commands::Task(TaskCommands::Down { namespace_id }) => {
                    commands::namespace::down(client, &namespace_id).await?;
                }
                Commands::Task(TaskCommands::Status { target }) => {
                    commands::namespace::status(client, &target).await?;
                }
                Commands::Task(TaskCommands::Logs {
                    namespace_id,
                    workload,
                    follow,
                }) => {
                    commands::streaming::logs(
                        client,
                        &namespace_id,
                        workload.as_deref(),
                        follow,
                    )
                    .await?;
                }
                Commands::Task(TaskCommands::Events {
                    namespace_id,
                    workload,
                    service,
                    follow,
                }) => {
                    commands::streaming::events(client, &namespace_id, &workload, &service, follow)
                        .await?;
                }
                Commands::Task(TaskCommands::Deactivate { target }) => {
                    commands::namespace::deactivate(client, &target).await?;
                }
                Commands::Task(TaskCommands::Splice {
                    namespace_id,
                    workload_id,
                    worker_id,
                }) => {
                    commands::splice::splice(client, &namespace_id, &workload_id, &worker_id)
                        .await?;
                }
                Commands::Task(TaskCommands::Clone { source, target }) => {
                    commands::namespace::clone_namespace(client, &source, &target).await?;
                }
                Commands::Resource(ResourceCommands::Get {
                    resource,
                    name: _,
                    namespace,
                    output,
                }) => {
                    commands::resource::get(client, &resource, namespace.as_deref(), &output)
                        .await?;
                }
                Commands::Resource(ResourceCommands::Describe {
                    resource,
                    name,
                    output,
                }) => {
                    commands::resource::describe(client, &resource, &name, &output).await?;
                }
                Commands::Resource(ResourceCommands::Create { resource }) => {
                    commands::resource::create(client, &resource).await?;
                }
                Commands::Resource(ResourceCommands::Delete { resource, name }) => {
                    commands::resource::delete(client, &resource, &name).await?;
                }
                Commands::Task(TaskCommands::Connect {
                    namespace_id,
                    config,
                }) => {
                    commands::connect::connect(client, &namespace_id, config).await?;
                }
                Commands::Task(TaskCommands::Disconnect { namespace_id }) => {
                    commands::connect::disconnect(client, &namespace_id).await?;
                }
                Commands::Auth(AuthCommands::Login { .. })
                | Commands::Auth(AuthCommands::Context { .. })
                | Commands::Other(OtherCommands::Legacy { .. }) => unreachable!(),
            }
        }
    }

    Ok(())
}
