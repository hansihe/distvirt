use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{braced, bracketed, parenthesized, Ident, Token, Type};

mod kw {
    syn::custom_keyword!(state_machines);
    syn::custom_keyword!(ports);
    syn::custom_keyword!(signals);
    syn::custom_keyword!(edges);
    syn::custom_keyword!(inputs);
    syn::custom_keyword!(sources);
    syn::custom_keyword!(aggregator);
    syn::custom_keyword!(events);
    syn::custom_keyword!(expose_internals_for_testing);
    syn::custom_keyword!(auto);
}

pub struct TopologyDef {
    pub expose_internals: bool,
    pub state_machines: Vec<SmDef>,
    pub ports: Vec<PortDef>,
    pub signals: Vec<SignalDef>,
    pub edges: Vec<EdgeDef>,
    pub events: Vec<EventDef>,
    pub inputs: Vec<InputDef>,
}

pub struct SmDef {
    pub name: Ident,
    /// None = auto-generated ID
    pub id_type: Option<Type>,
    pub handler_type: Type,
}

pub struct PortDef {
    pub name: Ident,
    /// None = auto-generated ID
    pub id_type: Option<Type>,
}

pub struct SignalDef {
    pub node: Ident,
    pub signal: Ident,
    pub value_type: Type,
}

pub struct EdgeDef {
    pub name: Ident,
    pub source: Ident,
    pub target: Ident,
}

pub struct InputDef {
    pub node: Ident,
    pub input_name: Ident,
    pub sources: Vec<SourcePair>,
    pub aggregator: Type,
}

// AdminCommand(AdminCommandPayload): ManagementPort -> WorkloadSm
pub struct EventDef {
    pub name: Ident,
    pub payload_type: Type,
    pub sender: Ident,
    pub receiver: Ident,
}

pub struct SourcePair {
    pub edge: Ident,
    pub node: Ident,
    pub signal: Ident,
}

impl Parse for TopologyDef {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut expose_internals = false;
        let mut state_machines = Vec::new();
        let mut ports = Vec::new();
        let mut signals = Vec::new();
        let mut edges = Vec::new();
        let mut events = Vec::new();
        let mut inputs = Vec::new();

        // Optional leading flag
        if input.peek(kw::expose_internals_for_testing) {
            input.parse::<kw::expose_internals_for_testing>()?;
            expose_internals = true;
        }

        while !input.is_empty() {
            let lookahead = input.lookahead1();
            if lookahead.peek(kw::state_machines) {
                input.parse::<kw::state_machines>()?;
                let content;
                braced!(content in input);
                state_machines =
                    Punctuated::<SmDef, Token![,]>::parse_terminated(&content)?
                        .into_iter()
                        .collect();
            } else if lookahead.peek(kw::ports) {
                input.parse::<kw::ports>()?;
                let content;
                braced!(content in input);
                ports = Punctuated::<PortDef, Token![,]>::parse_terminated(&content)?
                    .into_iter()
                    .collect();
            } else if lookahead.peek(kw::signals) {
                input.parse::<kw::signals>()?;
                let content;
                braced!(content in input);
                signals =
                    Punctuated::<SignalDef, Token![,]>::parse_terminated(&content)?
                        .into_iter()
                        .collect();
            } else if lookahead.peek(kw::edges) {
                input.parse::<kw::edges>()?;
                let content;
                braced!(content in input);
                edges = Punctuated::<EdgeDef, Token![,]>::parse_terminated(&content)?
                    .into_iter()
                    .collect();
            } else if lookahead.peek(kw::events) {
                input.parse::<kw::events>()?;
                let content;
                braced!(content in input);
                events =
                    Punctuated::<EventDef, Token![,]>::parse_terminated(&content)?
                        .into_iter()
                        .collect();
            } else if lookahead.peek(kw::inputs) {
                input.parse::<kw::inputs>()?;
                let content;
                braced!(content in input);
                inputs =
                    Punctuated::<InputDef, Token![,]>::parse_terminated(&content)?
                        .into_iter()
                        .collect();
            } else {
                return Err(lookahead.error());
            }
        }

        Ok(TopologyDef {
            expose_internals,
            state_machines,
            ports,
            signals,
            edges,
            events,
            inputs,
        })
    }
}

// Alpha(AlphaId, AlphaSm) or Alpha(auto, AlphaSm)
impl Parse for SmDef {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let name: Ident = input.parse()?;
        let content;
        parenthesized!(content in input);
        let id_type = if content.peek(kw::auto) {
            content.parse::<kw::auto>()?;
            None
        } else {
            Some(content.parse::<Type>()?)
        };
        content.parse::<Token![,]>()?;
        let handler_type: Type = content.parse()?;
        Ok(SmDef {
            name,
            id_type,
            handler_type,
        })
    }
}

// Gamma(GammaId) or Gamma(auto)
impl Parse for PortDef {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let name: Ident = input.parse()?;
        let content;
        parenthesized!(content in input);
        let id_type = if content.peek(kw::auto) {
            content.parse::<kw::auto>()?;
            None
        } else {
            Some(content.parse::<Type>()?)
        };
        Ok(PortDef { name, id_type })
    }
}

// Alpha::Demand(bool)
impl Parse for SignalDef {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let node: Ident = input.parse()?;
        input.parse::<Token![::]>()?;
        let signal: Ident = input.parse()?;
        let content;
        parenthesized!(content in input);
        let value_type: Type = content.parse()?;
        Ok(SignalDef {
            node,
            signal,
            value_type,
        })
    }
}

// AlphaToBeta: Alpha -> Beta
impl Parse for EdgeDef {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let name: Ident = input.parse()?;
        input.parse::<Token![:]>()?;
        let source: Ident = input.parse()?;
        input.parse::<Token![->]>()?;
        let target: Ident = input.parse()?;
        Ok(EdgeDef {
            name,
            source,
            target,
        })
    }
}

// Beta::DemandInput { sources: [(AlphaToBeta, Alpha::Demand)], aggregator: CountTrueAggregator }
impl Parse for InputDef {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let node: Ident = input.parse()?;
        input.parse::<Token![::]>()?;
        let input_name: Ident = input.parse()?;

        let content;
        braced!(content in input);

        // sources: [...]
        content.parse::<kw::sources>()?;
        content.parse::<Token![:]>()?;
        let sources_content;
        bracketed!(sources_content in content);
        let sources: Vec<SourcePair> =
            Punctuated::<SourcePair, Token![,]>::parse_terminated(&sources_content)?
                .into_iter()
                .collect();
        content.parse::<Token![,]>()?;

        // aggregator: Type
        content.parse::<kw::aggregator>()?;
        content.parse::<Token![:]>()?;
        let aggregator: Type = content.parse()?;

        // optional trailing comma
        let _ = content.parse::<Token![,]>();

        Ok(InputDef {
            node,
            input_name,
            sources,
            aggregator,
        })
    }
}

// AdminCommand(AdminCommandPayload): ManagementPort -> WorkloadSm
impl Parse for EventDef {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let name: Ident = input.parse()?;
        let content;
        parenthesized!(content in input);
        let payload_type: Type = content.parse()?;
        input.parse::<Token![:]>()?;
        let sender: Ident = input.parse()?;
        input.parse::<Token![->]>()?;
        let receiver: Ident = input.parse()?;
        Ok(EventDef {
            name,
            payload_type,
            sender,
            receiver,
        })
    }
}

// (AlphaToBeta, Alpha::Demand)
impl Parse for SourcePair {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let content;
        parenthesized!(content in input);
        let edge: Ident = content.parse()?;
        content.parse::<Token![,]>()?;
        let node: Ident = content.parse()?;
        content.parse::<Token![::]>()?;
        let signal: Ident = content.parse()?;
        Ok(SourcePair {
            edge,
            node,
            signal,
        })
    }
}
