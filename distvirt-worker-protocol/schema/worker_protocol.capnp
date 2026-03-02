@0xb7c5e3a1d9f24680;

# Worker protocol schema for communication between orchestrator and worker.
#
# Covers handshake messages, control stream commands/events, and log stream headers.

# --- Scalar Helpers ---

struct Ipv4Addr {
  raw @0 :UInt32;
  # Network byte order (big-endian). Convert via Ipv4Addr::from(u32::from_be(raw)).
}

struct MacAddr {
  b0 @0 :UInt8;
  b1 @1 :UInt8;
  b2 @2 :UInt8;
  b3 @3 :UInt8;
  b4 @4 :UInt8;
  b5 @5 :UInt8;
}

# --- Config Structs ---

struct NetworkConfig {
  subnet @0 :Ipv4Addr;
  gateway @1 :Ipv4Addr;
  prefixLen @2 :UInt8;
}

struct PodNetworkConfig {
  ip @0 :Ipv4Addr;
  mac @1 :MacAddr;
  gateway @2 :Ipv4Addr;
  netmask @3 :Text;
}

struct ContainerConfig {
  entrypoint @0 :Text;
  args @1 :List(Text);
  env @2 :List(Text);
  workingDir @3 :Text;        # empty string = not set
  hasUid @4 :Bool;
  uid @5 :UInt32;
  hasGid @6 :Bool;
  gid @7 :UInt32;
  hostname @8 :Text;          # empty string = not set
  captureOutput @9 :Bool;
}

struct ContainerSpec {
  containerId @0 :Text;
  imageRef @1 :Text;
  config @2 :ContainerConfig;
}

struct RegistryEntry {
  name @0 :Text;
  ip @1 :Ipv4Addr;
}

struct BufferPolicy {
  bufferFrames @0 :UInt32;
  timeoutMs @1 :UInt32;
}

struct ServicePolicy {
  bufferFrames @0 :UInt32;
  timeoutMs @1 :UInt32;
  hasActivator @2 :Bool;
  activator @3 :ActivatorConfig;
}

struct ActivatorConfig {
  union {
    tcp :group {
      hasPorts @0 :Bool;
      ports @1 :List(UInt16);
      tcpOnly @2 :Bool;
      maxFlows @3 :UInt32;
    }
    http2 @4 :Void;
  }
}

enum BackendNeed {
  none @0;
  traffic @1;
  active @2;
}

struct ServiceBackend {
  podIp @0 :Ipv4Addr;
  podMac @1 :MacAddr;
}

struct FabricRouteEntry {
  ip @0 :Ipv4Addr;
  mac @1 :MacAddr;
  destination @2 :RouteDestination;
}

struct RouteDestination {
  union {
    remoteWorker :group {
      workerId @0 :Text;
    }
    placeholder :group {
      bufferPolicy @1 :BufferPolicy;
    }
  }
}

# --- Handshake Messages ---

struct WorkerHello {
  authToken @0 :Text;
  capabilities @1 :WorkerCapabilities;
}

struct WorkerCapabilities {
  hasKvm @0 :Bool;
  hasContainerd @1 :Bool;
  availableAdapters @2 :List(Text);
}

struct WorkerAccepted {
  workerId @0 :Text;
  adapters @1 :List(AdapterConfig);
}

struct AdapterConfig {
  union {
    wireguard :group {
      listenPort @0 :UInt16;
      privateKey @1 :Data;  # 32 bytes
    }
    reverseProxy :group {
      listenPort @2 :UInt16;
      tlsCert @3 :Data;
      tlsKey @4 :Data;
    }
    osRouting :group {
      interface @5 :Text;
    }
  }
}

struct WorkerReady {
}

# --- Control Stream: Commands (orchestrator -> worker) ---

struct WorkerCommand {
  union {
    createNamespace :group {
      namespaceId @0 :Text;
      network @1 :NetworkConfig;
    }
    destroyNamespace :group {
      namespaceId @2 :Text;
    }
    registrySync :group {
      namespaceId @3 :Text;
      entries @4 :List(RegistryEntry);
    }
    registryUpdate :group {
      namespaceId @5 :Text;
      added @6 :List(RegistryEntry);
      removed @7 :List(Text);
    }
    launchPod :group {
      namespaceId @8 :Text;
      podId @9 :Text;
      network @10 :PodNetworkConfig;
      containers @11 :List(ContainerSpec);
    }
    stopPod :group {
      namespaceId @12 :Text;
      podId @13 :Text;
      graceful @14 :Bool;
    }
    fabricRouteSync :group {
      namespaceId @15 :Text;
      routes @16 :List(FabricRouteEntry);
    }
    fabricRouteUpdate :group {
      namespaceId @17 :Text;
      added @18 :List(FabricRouteEntry);
      removedIps @19 :List(Ipv4Addr);
    }
    createService :group {
      namespaceId @20 :Text;
      serviceId @21 :Text;
      ip @22 :Ipv4Addr;
      mac @23 :MacAddr;
      policy @24 :ServicePolicy;
    }
    updateServiceBackend :group {
      namespaceId @25 :Text;
      serviceId @26 :Text;
      hasBackend @27 :Bool;
      backend @28 :ServiceBackend;
    }
    serviceReady :group {
      namespaceId @29 :Text;
      serviceId @30 :Text;
    }
    destroyService :group {
      namespaceId @31 :Text;
      serviceId @32 :Text;
    }
    shutdown @33 :Void;
  }
}

# --- Control Stream: Events (worker -> orchestrator) ---

struct WorkerEvent {
  union {
    namespaceCreated :group {
      namespaceId @0 :Text;
    }
    namespaceFailed :group {
      namespaceId @1 :Text;
      error @2 :Text;
    }
    namespaceDestroyed :group {
      namespaceId @3 :Text;
    }
    podRunning :group {
      namespaceId @4 :Text;
      podId @5 :Text;
    }
    podExited :group {
      namespaceId @6 :Text;
      podId @7 :Text;
      exitCode @8 :Int32;
    }
    podFailed :group {
      namespaceId @9 :Text;
      podId @10 :Text;
      error @11 :Text;
    }
    shuttingDown @12 :Void;
    podLogStreamError :group {
      namespaceId @13 :Text;
      podId @14 :Text;
      containerId @15 :Text;
      phase @16 :Text;
      error @17 :Text;
    }
    serviceActivation :group {
      namespaceId @18 :Text;
      serviceId @19 :Text;
      dstIp @20 :Ipv4Addr;
    }
    serviceBackendNeed :group {
      namespaceId @21 :Text;
      serviceId @22 :Text;
      need @23 :BackendNeed;
    }
    fabricRouteMiss :group {
      namespaceId @24 :Text;
      dstIp @25 :Ipv4Addr;
      dstMac @26 :MacAddr;
    }
  }
}

# --- Log Stream Header ---

struct LogStreamHeader {
  namespaceId @0 :Text;
  podId @1 :Text;
  containerId @2 :Text;
}
