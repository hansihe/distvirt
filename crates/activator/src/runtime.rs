//! ActivatorRuntime: wasmtime Engine and Component loading.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use wasmtime::component::Component;
use wasmtime::{Config, Engine};

/// Holds a wasmtime Engine and pre-compiled Components by name.
pub struct ActivatorRuntime {
    engine: Engine,
    components: HashMap<String, Component>,
    component_dir: PathBuf,
}

impl ActivatorRuntime {
    /// Create a new runtime, scanning `component_dir` for `.wasm` files.
    ///
    /// Each file `foo.wasm` is loaded as component "foo".
    /// If `component_dir` does not exist, the runtime starts with no components.
    pub fn new(component_dir: &Path) -> Result<Self> {
        let mut config = Config::new();
        config.wasm_component_model(true);
        config.consume_fuel(true);

        let engine = Engine::new(&config).context("creating wasmtime engine")?;

        let mut runtime = ActivatorRuntime {
            engine,
            components: HashMap::new(),
            component_dir: component_dir.to_path_buf(),
        };

        if component_dir.is_dir() {
            runtime.load_components()?;
        } else {
            log::info!(
                "activator: component directory {:?} does not exist, no components loaded",
                component_dir
            );
        }

        Ok(runtime)
    }

    /// Load all `.wasm` files from the component directory.
    fn load_components(&mut self) -> Result<()> {
        let entries = std::fs::read_dir(&self.component_dir)
            .with_context(|| format!("reading component dir {:?}", self.component_dir))?;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("wasm") {
                let name = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("unknown")
                    .to_string();

                match Component::from_file(&self.engine, &path) {
                    Ok(component) => {
                        log::info!("activator: loaded component '{}' from {:?}", name, path);
                        self.components.insert(name, component);
                    }
                    Err(e) => {
                        log::error!(
                            "activator: failed to load component from {:?}: {:#}",
                            path,
                            e
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// Get a reference to the wasmtime Engine.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Get a pre-compiled component by name.
    pub fn get_component(&self, name: &str) -> Option<&Component> {
        self.components.get(name)
    }

    /// Check if a component is available.
    pub fn has_component(&self, name: &str) -> bool {
        self.components.contains_key(name)
    }

    /// List available component names.
    pub fn component_names(&self) -> impl Iterator<Item = &str> {
        self.components.keys().map(|s| s.as_str())
    }
}
