use clap::{Parser, Subcommand};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Connect to host:port, send data, print response
    Send {
        #[arg(long)]
        host: String,
        #[arg(long)]
        port: u16,
        #[arg(long)]
        data: String,
        #[arg(long, default_value = "10")]
        timeout: u64,
    },
    /// Listen on port, verify expected data, send response
    Recv {
        #[arg(long)]
        port: u16,
        #[arg(long)]
        expected: String,
        #[arg(long)]
        response: String,
        #[arg(long, default_value = "10")]
        timeout: u64,
    },
    /// Listen on port, send response to each connection
    Serve {
        #[arg(long)]
        port: u16,
        #[arg(long)]
        response: String,
        /// 0 = unlimited
        #[arg(long, default_value = "0")]
        max_connections: u32,
        /// Overall timeout in seconds
        #[arg(long, default_value = "300")]
        timeout: u64,
    },
    /// Allocate memory in steps to stress the balloon system
    MemStress {
        /// Total memory to allocate in MiB
        #[arg(long, default_value = "200")]
        target_mib: u64,
        /// Memory to allocate per step in MiB
        #[arg(long, default_value = "32")]
        step_mib: u64,
        /// Interval between steps in milliseconds
        #[arg(long, default_value = "1000")]
        interval_ms: u64,
    },
    /// Sleep until SIGTERM, then exit cleanly. Replaces /bin/sleep for PID1.
    Sleep,
    /// Print arguments to stdout and exit. Replaces /bin/echo.
    Echo {
        #[arg(trailing_var_arg = true)]
        args: Vec<String>,
    },
    /// Exit immediately with the given code. Replaces /bin/sh -c "exit N".
    ExitCode {
        #[arg(long)]
        code: i32,
    },
    /// Print environment variables and working directory. Replaces /bin/sh env-printing one-liners.
    EnvCheck {
        /// Environment variable names to print (as NAME=value)
        #[arg(long)]
        var: Vec<String>,
        /// Also print working directory
        #[arg(long, default_value = "false")]
        pwd: bool,
    },
    /// Perform a DNS lookup and print the result. Replaces /bin/sh nslookup one-liners.
    DnsLookup {
        #[arg(long)]
        host: String,
    },
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Command::Send {
            host,
            port,
            data,
            timeout,
        } => cmd_send(&host, port, &data, timeout),
        Command::Recv {
            port,
            expected,
            response,
            timeout,
        } => cmd_recv(port, &expected, &response, timeout),
        Command::Serve {
            port,
            response,
            max_connections,
            timeout,
        } => cmd_serve(port, &response, max_connections, timeout),
        Command::MemStress {
            target_mib,
            step_mib,
            interval_ms,
        } => cmd_mem_stress(target_mib, step_mib, interval_ms),
        Command::Sleep => cmd_sleep(),
        Command::Echo { args } => cmd_echo(&args),
        Command::ExitCode { code } => process::exit(code),
        Command::EnvCheck { var, pwd } => cmd_env_check(&var, pwd),
        Command::DnsLookup { host } => cmd_dns_lookup(&host),
    }
}

fn cmd_send(host: &str, port: u16, data: &str, timeout_secs: u64) {
    let timeout = Duration::from_secs(timeout_secs);
    let addr = format!("{host}:{port}");

    let mut stream = match TcpStream::connect_timeout(
        &addr.parse().unwrap_or_else(|e| {
            eprintln!("invalid address {addr}: {e}");
            process::exit(1);
        }),
        timeout,
    ) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("connection failed: {e}");
            process::exit(1);
        }
    };

    stream.set_read_timeout(Some(timeout)).unwrap();
    stream.set_write_timeout(Some(timeout)).unwrap();

    if let Err(e) = stream.write_all(data.as_bytes()) {
        eprintln!("send failed: {e}");
        process::exit(1);
    }

    if let Err(e) = stream.shutdown(std::net::Shutdown::Write) {
        eprintln!("shutdown write failed: {e}");
        process::exit(1);
    }

    let mut response = String::new();
    if let Err(e) = stream.read_to_string(&mut response) {
        eprintln!("read failed: {e}");
        process::exit(1);
    }

    print!("{response}");
}

fn cmd_serve(port: u16, response: &str, max_connections: u32, timeout_secs: u64) {
    let listener = match TcpListener::bind(format!("0.0.0.0:{port}")) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("bind failed: {e}");
            process::exit(1);
        }
    };

    // Set up SIGTERM handling — exit cleanly
    unsafe {
        libc::signal(libc::SIGTERM, {
            extern "C" fn handler(_: libc::c_int) {
                process::exit(0);
            }
            handler as libc::sighandler_t
        });
    }

    // Set accept timeout
    let timeout = Duration::from_secs(timeout_secs);
    unsafe {
        let tv = libc::timeval {
            tv_sec: timeout.as_secs() as libc::time_t,
            tv_usec: 0,
        };
        let ret = libc::setsockopt(
            std::os::unix::io::AsRawFd::as_raw_fd(&listener),
            libc::SOL_SOCKET,
            libc::SO_RCVTIMEO,
            &tv as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::timeval>() as libc::socklen_t,
        );
        if ret != 0 {
            eprintln!("setsockopt failed");
            process::exit(1);
        }
    }

    let mut connections: u32 = 0;
    loop {
        let (mut stream, _addr) = match listener.accept() {
            Ok(s) => s,
            Err(e) => {
                // Timeout or interrupt — exit cleanly
                eprintln!("accept ended: {e}");
                break;
            }
        };

        if let Err(e) = stream.write_all(response.as_bytes()) {
            eprintln!("write failed: {e}");
            // Continue serving other connections
        }
        let _ = stream.shutdown(std::net::Shutdown::Both);

        connections += 1;
        if max_connections > 0 && connections >= max_connections {
            break;
        }
    }
}

fn cmd_mem_stress(target_mib: u64, step_mib: u64, interval_ms: u64) {
    static SHOULD_EXIT: AtomicBool = AtomicBool::new(false);

    // Set up SIGTERM handler for clean exit.
    unsafe {
        libc::signal(libc::SIGTERM, {
            extern "C" fn handler(_: libc::c_int) {
                SHOULD_EXIT.store(true, Ordering::SeqCst);
            }
            handler as libc::sighandler_t
        });
    }

    let step_bytes = step_mib as usize * 1024 * 1024;
    let target_bytes = target_mib as usize * 1024 * 1024;
    let interval = Duration::from_millis(interval_ms);

    println!(
        "mem-stress: target={}MiB step={}MiB interval={}ms",
        target_mib, step_mib, interval_ms
    );

    // Hold all allocations so they aren't freed.
    let mut allocations: Vec<Vec<u8>> = Vec::new();
    let mut total_allocated: usize = 0;

    while total_allocated < target_bytes {
        if SHOULD_EXIT.load(Ordering::SeqCst) {
            println!("mem-stress: received SIGTERM, exiting");
            return;
        }

        let chunk_size = step_bytes.min(target_bytes - total_allocated);
        let mut chunk = vec![0u8; chunk_size];

        // Touch every page (4KiB) to force physical allocation.
        for i in (0..chunk_size).step_by(4096) {
            chunk[i] = 0xAA;
        }

        total_allocated += chunk_size;
        allocations.push(chunk);

        // Read RSS from /proc/self/statm (field 1, in pages).
        let rss_kib = read_rss_kib();
        println!(
            "mem-stress: allocated={}MiB rss={}MiB",
            total_allocated / (1024 * 1024),
            rss_kib / 1024
        );

        std::thread::sleep(interval);
    }

    println!(
        "mem-stress: target reached ({}MiB), holding allocations",
        target_mib
    );

    // Hold until SIGTERM.
    while !SHOULD_EXIT.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_secs(1));
    }

    println!("mem-stress: received SIGTERM, freeing and exiting");
    drop(allocations);
}

fn read_rss_kib() -> usize {
    let Ok(statm) = std::fs::read_to_string("/proc/self/statm") else {
        return 0;
    };
    // statm fields: size resident shared text lib data dt (all in pages)
    let rss_pages: usize = statm
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    // Page size is typically 4KiB.
    rss_pages * 4
}

fn cmd_sleep() {
    unsafe {
        libc::signal(libc::SIGTERM, {
            extern "C" fn handler(_: libc::c_int) {
                process::exit(0);
            }
            handler as libc::sighandler_t
        });
    }
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}

fn cmd_echo(args: &[String]) {
    println!("{}", args.join(" "));
}

fn cmd_env_check(vars: &[String], pwd: bool) {
    for name in vars {
        let val = std::env::var(name).unwrap_or_default();
        println!("{name}={val}");
    }
    if pwd {
        println!("{}", std::env::current_dir().unwrap().display());
    }
}

fn cmd_dns_lookup(host: &str) {
    use std::net::ToSocketAddrs;
    match (host, 0u16).to_socket_addrs() {
        Ok(addrs) => {
            for addr in addrs {
                println!("{}", addr.ip());
            }
        }
        Err(e) => {
            eprintln!("dns lookup failed for {host}: {e}");
            // Exit 0 like the original `nslookup ... || true`
        }
    }
}

fn cmd_recv(port: u16, expected: &str, response: &str, timeout_secs: u64) {
    let timeout = Duration::from_secs(timeout_secs);

    let listener = match TcpListener::bind(format!("0.0.0.0:{port}")) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("bind failed: {e}");
            process::exit(1);
        }
    };

    // Set accept timeout via SO_RCVTIMEO
    unsafe {
        let tv = libc::timeval {
            tv_sec: timeout.as_secs() as libc::time_t,
            tv_usec: 0,
        };
        let ret = libc::setsockopt(
            std::os::unix::io::AsRawFd::as_raw_fd(&listener),
            libc::SOL_SOCKET,
            libc::SO_RCVTIMEO,
            &tv as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::timeval>() as libc::socklen_t,
        );
        if ret != 0 {
            eprintln!("setsockopt failed");
            process::exit(1);
        }
    }

    let (mut stream, _addr) = match listener.accept() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("accept failed (timeout?): {e}");
            process::exit(1);
        }
    };

    stream.set_read_timeout(Some(timeout)).unwrap();
    stream.set_write_timeout(Some(timeout)).unwrap();

    let mut received = String::new();
    if let Err(e) = stream.read_to_string(&mut received) {
        eprintln!("read failed: {e}");
        process::exit(1);
    }

    if received != expected {
        eprintln!("data mismatch: expected {expected:?}, got {received:?}");
        process::exit(1);
    }

    if let Err(e) = stream.write_all(response.as_bytes()) {
        eprintln!("send response failed: {e}");
        process::exit(1);
    }
}
