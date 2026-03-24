use std::path::PathBuf;

use clap::{Parser, Subcommand};

mod commands;
mod connection;
pub mod entity_ref;
mod format;
mod status_watch;

use commands::OutputFormat;
use entity_ref::{EntityRefSpec, ResourceType};

const GROUPED_HELP: &str = "\x1b[1;4mTask commands:\x1b[0m
  down          Delete a namespace
  status        Show namespace/workload status
  logs          Stream logs from a namespace
  events        Stream events from a namespace
  deactivate    Hint the orchestrator to deactivate a workload
  connect       Connect to a namespace network via WireGuard
  disconnect    Disconnect from a namespace network
  clone         Clone a namespace
  attach        Attach to a running workload's I/O
  splice        Splice a workload to a local worker

\x1b[1;4mSpec commands:\x1b[0m
  spec apply    Apply a spec (create namespace or patch existing)
  spec sync     Sync a spec (create namespace or replace existing)
  spec validate Validate a spec file
  spec render   Render a spec file to resolved proto JSON

\x1b[1;4mResource commands:\x1b[0m
  get           List resources
  describe      Describe a resource in detail
  delete        Delete a resource

\x1b[1;4mAuth & config:\x1b[0m
  login         Log in and save credentials
  context       Manage named contexts";

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

    /// Log level (off, error, warn, info, debug, trace)
    #[arg(long, global = true)]
    log_level: Option<log::LevelFilter>,

    /// Enable verbose (debug) logging
    #[arg(short, long, global = true)]
    verbose: bool,

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
    /// Internal commands (not for direct use)
    #[command(hide = true)]
    Internal {
        #[command(subcommand)]
        command: InternalCommands,
    },
}

#[derive(Subcommand)]
enum InternalCommands {
    /// Create a TUN device and pass the fd back to the parent process
    SetupTun {
        /// Path to the Unix domain socket
        #[arg(long)]
        socket: PathBuf,
        /// Nonce for authentication
        #[arg(long)]
        nonce: String,
        /// Client IP address
        #[arg(long)]
        client_ip: String,
        /// Prefix length
        #[arg(long)]
        prefix_len: u8,
        /// Subnet CIDR
        #[arg(long)]
        subnet: String,
    },
}

/// Layer 1 — task-oriented commands
#[derive(Subcommand)]
enum TaskCommands {
    /// Spec file operations (apply, sync, validate, render)
    #[command(hide = true)]
    Spec {
        #[command(subcommand)]
        command: SpecCommands,
    },
    /// Delete a namespace
    #[command(hide = true)]
    Down {
        /// Entity reference: /namespace
        target: String,
    },
    /// Show live namespace status
    #[command(hide = true)]
    Status {
        /// Entity reference: /namespace or /namespace/wl/name
        target: String,
        /// Watch: show status then stream events
        #[arg(short, long)]
        watch: bool,
    },
    /// Stream logs from a namespace
    #[command(hide = true)]
    Logs {
        /// Entity reference: /namespace or /namespace/wl/name
        target: String,
        /// Follow log output
        #[arg(short, long)]
        follow: bool,
    },
    /// Stream events from a namespace
    #[command(hide = true)]
    Events {
        /// Entity references: /namespace, /namespace/wl/name, /namespace/svc/name
        #[arg(required = true)]
        targets: Vec<String>,
        /// Follow event stream
        #[arg(short, long)]
        follow: bool,
    },
    /// Hint the orchestrator to deactivate a workload immediately
    #[command(hide = true)]
    Deactivate {
        /// Entity reference: /namespace/wl/name
        target: String,
    },
    /// Connect to a namespace network via WireGuard tunnel
    #[command(hide = true)]
    Connect {
        /// Entity reference: /namespace
        target: String,
        /// Print wg-quick config instead of establishing tunnel
        #[arg(long)]
        config: bool,
    },
    /// Disconnect from a namespace network
    #[command(hide = true)]
    Disconnect {
        /// Entity reference: /namespace
        target: String,
    },
    /// Attach to a running workload's stdin/stdout/stderr
    #[command(hide = true)]
    Attach {
        /// Entity reference: /namespace/wl/name
        target: String,
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
    /// List or describe resources
    #[command(hide = true)]
    Get {
        /// Entity reference or global type: /ns/wl, /ns/wl/name, namespaces, workers/name
        target: String,
        /// Output format
        #[arg(short, long, value_enum, default_value_t = OutputFormat::Text)]
        output: OutputFormat,
    },
    /// Describe a resource in detail
    #[command(hide = true)]
    Describe {
        /// Entity reference or global type: /namespace, namespaces/name, workers/name
        target: String,
        /// Output format
        #[arg(short, long, value_enum, default_value_t = OutputFormat::Text)]
        output: OutputFormat,
    },
    /// Delete a resource
    #[command(hide = true)]
    Delete {
        /// Entity reference or global type: /namespace, /ns/wl/name, /ns/svc/name
        target: String,
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

#[derive(Subcommand)]
enum SpecCommands {
    /// Apply a spec: create namespace if new, patch (upsert) workloads/services if existing
    Apply {
        /// Namespace ID (optional if spec file has metadata.name)
        namespace_id: Option<String>,
        /// Path to spec file (distvirt.yaml)
        #[arg(short, long)]
        file: Option<PathBuf>,
        /// Label selector to filter which workloads/services to apply (e.g. "env=staging,team=platform")
        #[arg(short = 'l', long = "selector")]
        selector: Option<String>,
    },
    /// Sync a spec: create namespace if new, fully replace spec if existing
    Sync {
        /// Namespace ID (optional if spec file has metadata.name)
        namespace_id: Option<String>,
        /// Path to spec file (distvirt.yaml)
        #[arg(short, long)]
        file: Option<PathBuf>,
    },
    /// Validate a spec file (parse, resolve includes, check for errors)
    Validate {
        /// Path to spec file
        #[arg(short, long)]
        file: Option<PathBuf>,
    },
    /// Render a spec file to resolved proto JSON
    Render {
        /// Path to spec file
        #[arg(short, long)]
        file: PathBuf,
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
    let cli = Cli::parse();

    let default_level = if cli.verbose {
        log::LevelFilter::Debug
    } else {
        cli.log_level.unwrap_or(log::LevelFilter::Info)
    };

    env_logger::Builder::new()
        .filter_level(default_level)
        .parse_default_env()
        .init();

    match cli.command {
        // Internal commands — privileged helpers, no gRPC needed
        Commands::Internal {
            command:
                InternalCommands::SetupTun {
                    socket,
                    nonce,
                    client_ip,
                    prefix_len,
                    subnet,
                },
        } => {
            return commands::internal::setup_tun(socket, nonce, client_ip, prefix_len, subnet);
        }

        // Auth commands — no gRPC needed
        Commands::Auth(AuthCommands::Login { server, token }) => {
            commands::auth::login(server.as_deref(), token.as_deref(), cli.context.as_deref())?;
        }
        Commands::Auth(AuthCommands::Context { command }) => match command {
            None => commands::auth::context_show()?,
            Some(ContextCommands::Use { name }) => commands::auth::context_use(&name)?,
            Some(ContextCommands::List) => commands::auth::context_list()?,
            Some(ContextCommands::Delete { name }) => commands::auth::context_delete(&name)?,
        },

        // Spec commands that run locally, no gRPC needed
        Commands::Task(TaskCommands::Spec {
            command: SpecCommands::Validate { file },
        }) => {
            commands::namespace::validate(file.as_deref())?;
        }
        Commands::Task(TaskCommands::Spec {
            command: SpecCommands::Render { file },
        }) => {
            commands::namespace::render(&file)?;
        }

        // All other commands connect to the orchestrator
        cmd => {
            let params = connection::resolve(
                cli.server.as_deref(),
                cli.token.as_deref(),
                cli.context.as_deref(),
            )?;
            let client = distvirt_client::connection::connect(&params).await?;

            match cmd {
                Commands::Task(TaskCommands::Spec {
                    command: SpecCommands::Apply { namespace_id, file, selector },
                }) => {
                    commands::namespace::apply(client, namespace_id.as_deref(), file.as_deref(), selector.as_deref())
                        .await?;
                }
                Commands::Task(TaskCommands::Spec {
                    command: SpecCommands::Sync { namespace_id, file },
                }) => {
                    commands::namespace::sync(client, namespace_id.as_deref(), file.as_deref())
                        .await?;
                }
                Commands::Task(TaskCommands::Down { target }) => {
                    let spec = EntityRefSpec::new("down")
                        .accept_namespace();
                    let resolved = entity_ref::parse_and_resolve(&target, &spec, None)?;
                    commands::namespace::down(client, resolved.namespace()).await?;
                }
                Commands::Task(TaskCommands::Status { target, watch }) => {
                    let spec = EntityRefSpec::new("status")
                        .accept_namespace()
                        .accept_resource_of(&[ResourceType::Workload]);
                    let resolved = entity_ref::parse_and_resolve(&target, &spec, None)?;
                    commands::namespace::status(client, resolved.namespace(), resolved.name(), watch)
                        .await?;
                }
                Commands::Task(TaskCommands::Logs { target, follow }) => {
                    let spec = EntityRefSpec::new("logs")
                        .default_type(ResourceType::Workload)
                        .accept_namespace()
                        .accept_resource_of(&[ResourceType::Workload]);
                    let resolved = entity_ref::parse_and_resolve(&target, &spec, None)?;
                    commands::streaming::logs(
                        client,
                        resolved.namespace(),
                        resolved.name(),
                        follow,
                    )
                    .await?;
                }
                Commands::Task(TaskCommands::Events { targets, follow }) => {
                    let spec = EntityRefSpec::new("events")
                        .default_type(ResourceType::Workload)
                        .accept_namespace()
                        .accept_resource_of(&[
                            ResourceType::Workload,
                            ResourceType::Service,
                        ]);
                    let resolved: Vec<_> = targets
                        .iter()
                        .map(|t| entity_ref::parse_and_resolve(t, &spec, None))
                        .collect::<Result<_, _>>()?;

                    let namespace_id = resolved
                        .first()
                        .expect("clap requires at least one target")
                        .namespace();
                    for r in &resolved {
                        if r.namespace() != namespace_id {
                            anyhow::bail!(
                                "all targets must be in the same namespace (got {} and {})",
                                resolved[0].path(),
                                r.path()
                            );
                        }
                    }

                    let mut workloads = Vec::new();
                    let mut services = Vec::new();
                    for r in &resolved {
                        if let Some(name) = r.name() {
                            match r.resource_type() {
                                Some(ResourceType::Workload) => {
                                    workloads.push(name.to_string())
                                }
                                Some(ResourceType::Service) => {
                                    services.push(name.to_string())
                                }
                                _ => {}
                            }
                        }
                    }

                    commands::streaming::events(
                        client,
                        namespace_id,
                        &workloads,
                        &services,
                        follow,
                    )
                    .await?;
                }
                Commands::Task(TaskCommands::Deactivate { target }) => {
                    let spec = EntityRefSpec::new("deactivate")
                        .default_type(ResourceType::Workload)
                        .accept_resource_of(&[ResourceType::Workload]);
                    let resolved = entity_ref::parse_and_resolve(&target, &spec, None)?;
                    commands::namespace::deactivate(
                        client,
                        resolved.namespace(),
                        resolved.name().expect("validated by spec"),
                    )
                    .await?;
                }
                Commands::Task(TaskCommands::Attach { target }) => {
                    let spec = EntityRefSpec::new("attach")
                        .default_type(ResourceType::Workload)
                        .accept_resource_of(&[ResourceType::Workload]);
                    let resolved = entity_ref::parse_and_resolve(&target, &spec, None)?;
                    commands::attach::attach(
                        client,
                        resolved.namespace(),
                        resolved.name().expect("validated by spec"),
                    )
                    .await?;
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
                Commands::Resource(ResourceCommands::Get { target, output }) => {
                    if let Some((type_name, name)) = parse_global_resource(&target) {
                        if let Some(name) = name {
                            commands::resource::describe(client, type_name, name, &output)
                                .await?;
                        } else {
                            commands::resource::get(client, type_name, None, &output).await?;
                        }
                    } else {
                        let spec = EntityRefSpec::new("get")
                            .accept_namespace()
                            .accept_type_of(&[
                                ResourceType::Workload,
                                ResourceType::Service,
                                ResourceType::Pod,
                            ])
                            .accept_resource_of(&[
                                ResourceType::Workload,
                                ResourceType::Service,
                                ResourceType::Pod,
                            ]);
                        let resolved =
                            entity_ref::parse_and_resolve(&target, &spec, None)?;
                        match &resolved {
                            entity_ref::ResolvedRef::Namespace(ns) => {
                                commands::resource::describe(
                                    client,
                                    "namespaces",
                                    ns,
                                    &output,
                                )
                                .await?;
                            }
                            entity_ref::ResolvedRef::TypeInNamespace(ns, rt) => {
                                commands::resource::get(
                                    client,
                                    rt.plural(),
                                    Some(ns.as_str()),
                                    &output,
                                )
                                .await?;
                            }
                            entity_ref::ResolvedRef::Resource(ns, rt, name) => {
                                commands::resource::describe_namespaced(
                                    client,
                                    ns,
                                    *rt,
                                    name,
                                    &output,
                                )
                                .await?;
                            }
                        }
                    }
                }
                Commands::Resource(ResourceCommands::Describe { target, output }) => {
                    if let Some((type_name, name)) = parse_global_resource(&target) {
                        let name = name.ok_or_else(|| {
                            anyhow::anyhow!(
                                "`describe` requires a name. Try: dv describe {}/name",
                                type_name
                            )
                        })?;
                        commands::resource::describe(client, type_name, name, &output)
                            .await?;
                    } else {
                        let spec = EntityRefSpec::new("describe")
                            .accept_namespace();
                        let resolved =
                            entity_ref::parse_and_resolve(&target, &spec, None)?;
                        commands::resource::describe(
                            client,
                            "namespaces",
                            resolved.namespace(),
                            &output,
                        )
                        .await?;
                    }
                }
                Commands::Resource(ResourceCommands::Delete { target }) => {
                    if let Some((type_name, name)) = parse_global_resource(&target) {
                        let name = name.ok_or_else(|| {
                            anyhow::anyhow!(
                                "`delete` requires a name. Try: dv delete {}/name",
                                type_name
                            )
                        })?;
                        commands::resource::delete(client, type_name, name, None).await?;
                    } else {
                        let spec = EntityRefSpec::new("delete")
                            .accept_namespace()
                            .accept_resource_of(&[
                                ResourceType::Workload,
                                ResourceType::Service,
                            ]);
                        let resolved =
                            entity_ref::parse_and_resolve(&target, &spec, None)?;
                        match &resolved {
                            entity_ref::ResolvedRef::Namespace(ns) => {
                                commands::resource::delete(
                                    client,
                                    "namespaces",
                                    ns,
                                    None,
                                )
                                .await?;
                            }
                            entity_ref::ResolvedRef::Resource(ns, rt, name) => {
                                commands::resource::delete(
                                    client,
                                    rt.plural(),
                                    name,
                                    Some(ns.as_str()),
                                )
                                .await?;
                            }
                            _ => unreachable!("spec only accepts Namespace and Resource"),
                        }
                    }
                }
                Commands::Task(TaskCommands::Connect { target, config }) => {
                    let spec = EntityRefSpec::new("connect")
                        .accept_namespace();
                    let resolved = entity_ref::parse_and_resolve(&target, &spec, None)?;
                    commands::connect::connect(client, &params, resolved.namespace(), config)
                        .await?;
                }
                Commands::Task(TaskCommands::Disconnect { target }) => {
                    let spec = EntityRefSpec::new("disconnect")
                        .accept_namespace();
                    let resolved = entity_ref::parse_and_resolve(&target, &spec, None)?;
                    commands::connect::disconnect(client, resolved.namespace()).await?;
                }
                Commands::Auth(AuthCommands::Login { .. })
                | Commands::Auth(AuthCommands::Context { .. })
                | Commands::Internal { .. }
                | Commands::Task(TaskCommands::Spec {
                    command: SpecCommands::Validate { .. } | SpecCommands::Render { .. },
                }) => unreachable!(),
            }
        }
    }

    Ok(())
}

/// Check if the input refers to a global (non-namespaced) resource type.
/// Returns the normalized type name and optional resource name.
fn parse_global_resource(input: &str) -> Option<(&'static str, Option<&str>)> {
    if input.starts_with('/') {
        return None;
    }
    let (first, rest) = match input.split_once('/') {
        Some((f, r)) => (f, Some(r)),
        None => (input, None),
    };
    let normalized = match first {
        "namespace" | "namespaces" | "ns" => "namespaces",
        "worker" | "workers" => "workers",
        _ => return None,
    };
    Some((normalized, rest))
}
