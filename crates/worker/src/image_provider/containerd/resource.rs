/// A typed reference to a snapshot in a specific snapshotter.
#[derive(Debug, Clone)]
pub struct SnapshotRef {
    pub snapshotter: String,
    pub key: String,
}

/// A typed reference to content in the content store.
#[derive(Debug, Clone)]
pub struct ContentRef(pub String);

/// Trait for types that can be registered as containerd lease resources.
pub trait LeaseResource {
    /// The resource type string for the containerd lease API.
    fn resource_type(&self) -> String;

    /// The resource ID (snapshot key or content digest).
    fn resource_id(&self) -> &str;
}

impl LeaseResource for SnapshotRef {
    fn resource_type(&self) -> String {
        format!("snapshots/{}", self.snapshotter)
    }

    fn resource_id(&self) -> &str {
        &self.key
    }
}

impl LeaseResource for ContentRef {
    fn resource_type(&self) -> String {
        "content".to_string()
    }

    fn resource_id(&self) -> &str {
        &self.0
    }
}
