use crate::config;

pub fn login(server: Option<&str>, token: Option<&str>) -> anyhow::Result<()> {
    let server = server
        .ok_or_else(|| anyhow::anyhow!("--server is required for login"))?;
    let token = token
        .ok_or_else(|| anyhow::anyhow!("--token is required for login"))?;

    let mut creds = config::load()?;

    let context_name = &creds.current_context.clone();

    creds.contexts.insert(
        context_name.clone(),
        config::Context {
            server: server.to_string(),
            token: token.to_string(),
            tls: None,
        },
    );

    config::save(&creds)?;
    eprintln!("logged in to {} (context '{}')", server, context_name);
    Ok(())
}

pub fn context_show() -> anyhow::Result<()> {
    let creds = config::load()?;
    let name = &creds.current_context;

    if let Some(ctx) = creds.contexts.get(name) {
        println!("{}  (server: {})", name, ctx.server);
    } else {
        println!("{} (not configured)", name);
    }
    Ok(())
}

pub fn context_use(name: &str) -> anyhow::Result<()> {
    let mut creds = config::load()?;

    if !creds.contexts.contains_key(name) {
        anyhow::bail!("context '{}' does not exist", name);
    }

    creds.current_context = name.to_string();
    config::save(&creds)?;
    eprintln!("switched to context '{}'", name);
    Ok(())
}

pub fn context_list() -> anyhow::Result<()> {
    let creds = config::load()?;

    if creds.contexts.is_empty() {
        println!("no contexts configured");
        return Ok(());
    }

    for (name, ctx) in &creds.contexts {
        let marker = if name == &creds.current_context { "*" } else { " " };
        println!("{} {:<20} {}", marker, name, ctx.server);
    }
    Ok(())
}

pub fn context_delete(name: &str) -> anyhow::Result<()> {
    let mut creds = config::load()?;

    if creds.contexts.remove(name).is_none() {
        anyhow::bail!("context '{}' does not exist", name);
    }

    if creds.current_context == name {
        creds.current_context = "default".to_string();
    }

    config::save(&creds)?;
    eprintln!("deleted context '{}'", name);
    Ok(())
}
