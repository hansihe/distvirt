use super::*;
use distvirt_sm_router::SmHandler;

// ---- Service SM ----

/// Service SM — now a thin wrapper that creates and configures an EndpointSm.
///
/// Receives service spec from management, creates an Endpoint SM,
/// pushes config to it, and forwards activation events.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ServiceSm {
    pub endpoint_id: Option<EndpointId>,
}

impl ServiceSm {
    pub(crate) fn new() -> Self {
        ServiceSm { endpoint_id: None }
    }
}

impl<C: ServiceCtx> SmHandler<C> for ServiceSm {
    type Input = ServiceInput;

    fn handle(&mut self, input: Self::Input, ctx: &mut C) {
        match input {
            ServiceInput::SvcSpecInput(spec_opt) => {
                if let Some((_, spec)) = spec_opt {
                    // Create endpoint if needed.
                    if self.endpoint_id.is_none() {
                        let ep_id =
                            ctx.create_endpoint(endpoint::EndpointSm::new(spec.has_activation));
                        self.endpoint_id = Some(ep_id);
                        ctx.set_service_endpoint_ownership_edges(vec![ep_id]);
                    }
                    // Push config to endpoint via signal.
                    ctx.set_endpoint_config(Some(endpoint::EndpointConfig {
                        kind: endpoint::EndpointKind::Service {
                            service_id: ctx.id(),
                        },
                        workload: spec.workload,
                        has_activation: spec.has_activation,
                        idle_timeout: spec.idle_timeout,
                        ip: spec.ip,
                        policy: spec.policy,
                        dns_entry: match (spec.dns_name, spec.dns_ip) {
                            (Some(name), Some(ip)) => Some(DnsEntryInfo { name, ip }),
                            _ => None,
                        },
                    }));
                } else {
                    // Spec removed — self-destruct.
                    // Endpoint will self-destruct when it loses its config.
                    ctx.self_destruct();
                }
            }
            ServiceInput::ActivateService(active) => {
                // Forward to endpoint.
                if let Some(ep_id) = self.endpoint_id {
                    ctx.send_activate_endpoint(ep_id, active);
                }
            }
        }
    }
}
