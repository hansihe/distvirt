use std::fmt;

// ============================================================
// Resource Types
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResourceType {
    Workload,
    Service,
    Pod,
}

impl ResourceType {
    pub fn from_keyword(s: &str) -> Option<Self> {
        match s {
            "workload" | "wl" => Some(Self::Workload),
            "service" | "svc" => Some(Self::Service),
            "pod" | "po" => Some(Self::Pod),
            _ => None,
        }
    }

    /// Short name for use in refs: `/ns/wl/name`
    pub fn short(&self) -> &'static str {
        match self {
            Self::Workload => "wl",
            Self::Service => "svc",
            Self::Pod => "po",
        }
    }

    /// Canonical singular name: "workload"
    pub fn canonical(&self) -> &'static str {
        match self {
            Self::Workload => "workload",
            Self::Service => "service",
            Self::Pod => "pod",
        }
    }

    /// Plural name: "workloads"
    pub fn plural(&self) -> &'static str {
        match self {
            Self::Workload => "workloads",
            Self::Service => "services",
            Self::Pod => "pods",
        }
    }

    pub fn all() -> &'static [ResourceType] {
        &[Self::Workload, Self::Service, Self::Pod]
    }

    fn all_keywords_display() -> String {
        Self::all()
            .iter()
            .map(|t| format!("{} ({})", t.canonical(), t.short()))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl fmt::Display for ResourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.canonical())
    }
}

// ============================================================
// Resolved Reference
// ============================================================

/// A fully resolved entity reference with namespace, optional type, and optional name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedRef {
    Namespace(String),
    TypeInNamespace(String, ResourceType),
    Resource(String, ResourceType, String),
}

impl ResolvedRef {
    pub fn namespace(&self) -> &str {
        match self {
            Self::Namespace(ns) | Self::TypeInNamespace(ns, _) | Self::Resource(ns, _, _) => ns,
        }
    }

    pub fn resource_type(&self) -> Option<ResourceType> {
        match self {
            Self::Namespace(_) => None,
            Self::TypeInNamespace(_, rt) | Self::Resource(_, rt, _) => Some(*rt),
        }
    }

    pub fn name(&self) -> Option<&str> {
        match self {
            Self::Resource(_, _, name) => Some(name),
            _ => None,
        }
    }

    pub fn form(&self) -> RefForm {
        match self {
            Self::Namespace(_) => RefForm::Namespace,
            Self::TypeInNamespace(_, _) => RefForm::TypeInNamespace,
            Self::Resource(_, _, _) => RefForm::Resource,
        }
    }

    /// Path representation: `/ns/type/name`
    pub fn path(&self) -> String {
        match self {
            Self::Namespace(ns) => format!("/{ns}"),
            Self::TypeInNamespace(ns, rt) => format!("/{ns}/{}", rt.short()),
            Self::Resource(ns, rt, name) => format!("/{ns}/{}/{name}", rt.short()),
        }
    }

    /// Human-readable description for use in prose.
    pub fn describe(&self) -> String {
        match self {
            Self::Namespace(ns) => format!("namespace \"{ns}\""),
            Self::TypeInNamespace(ns, rt) => {
                format!("{} in namespace \"{ns}\"", rt.plural())
            }
            Self::Resource(ns, rt, name) => {
                format!("{} \"{ns}/{name}\"", rt.canonical())
            }
        }
    }
}

impl fmt::Display for ResolvedRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.path())
    }
}

// ============================================================
// Reference Form
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefForm {
    Namespace,
    TypeInNamespace,
    Resource,
}

impl RefForm {
    fn describe(self) -> &'static str {
        match self {
            Self::Namespace => "a namespace reference",
            Self::TypeInNamespace => "a type reference",
            Self::Resource => "a specific resource reference",
        }
    }
}

// ============================================================
// Entity Reference Spec (per-command configuration)
// ============================================================

/// Declares what forms of entity reference a command accepts.
///
/// Build with chained methods:
/// ```ignore
/// EntityRefSpec::new("logs")
///     .default_type(ResourceType::Workload)
///     .accept_resource_of(&[ResourceType::Workload])
/// ```
#[derive(Debug, Clone)]
pub struct EntityRefSpec {
    command_name: String,
    accepted: Vec<AcceptedForm>,
    default_type: Option<ResourceType>,
}

#[derive(Debug, Clone)]
pub enum AcceptedForm {
    /// Accepts a bare namespace: `/my-ns`
    Namespace,
    /// Accepts a type within a namespace: `/my-ns/wl`
    TypeInNamespace { types: Option<Vec<ResourceType>> },
    /// Accepts a specific resource: `/my-ns/wl/my-app`
    Resource { types: Option<Vec<ResourceType>> },
}

impl AcceptedForm {
    fn matches_form(&self, form: RefForm) -> bool {
        matches!(
            (self, form),
            (AcceptedForm::Namespace, RefForm::Namespace)
                | (AcceptedForm::TypeInNamespace { .. }, RefForm::TypeInNamespace)
                | (AcceptedForm::Resource { .. }, RefForm::Resource)
        )
    }

    fn type_constraint(&self) -> Option<&[ResourceType]> {
        match self {
            AcceptedForm::Namespace => None,
            AcceptedForm::TypeInNamespace { types } | AcceptedForm::Resource { types } => {
                types.as_deref()
            }
        }
    }

    fn format_pattern(&self) -> String {
        match self {
            AcceptedForm::Namespace => "/namespace".into(),
            AcceptedForm::TypeInNamespace { types } => {
                format!("/namespace/{}", format_type_placeholder(types.as_deref()))
            }
            AcceptedForm::Resource { types } => {
                format!(
                    "/namespace/{}/name",
                    format_type_placeholder(types.as_deref())
                )
            }
        }
    }
}

impl EntityRefSpec {
    pub fn new(command_name: &str) -> Self {
        Self {
            command_name: command_name.to_string(),
            accepted: Vec::new(),
            default_type: None,
        }
    }

    /// Set a default resource type for bare name resolution.
    ///
    /// When a user types just `my-app` (no type, no namespace),
    /// this type is assumed. For example, `logs` defaults to `Workload`.
    pub fn default_type(mut self, t: ResourceType) -> Self {
        self.default_type = Some(t);
        self
    }

    /// Accept bare namespace references: `/my-ns`
    pub fn accept_namespace(mut self) -> Self {
        self.accepted.push(AcceptedForm::Namespace);
        self
    }

    /// Accept type-in-namespace references with any type: `/my-ns/wl`
    pub fn accept_type_any(mut self) -> Self {
        self.accepted
            .push(AcceptedForm::TypeInNamespace { types: None });
        self
    }

    /// Accept type-in-namespace references constrained to specific types.
    pub fn accept_type_of(mut self, types: &[ResourceType]) -> Self {
        self.accepted.push(AcceptedForm::TypeInNamespace {
            types: Some(types.to_vec()),
        });
        self
    }

    /// Accept specific resource references with any type: `/my-ns/wl/my-app`
    pub fn accept_resource_any(mut self) -> Self {
        self.accepted.push(AcceptedForm::Resource { types: None });
        self
    }

    /// Accept specific resource references constrained to specific types.
    pub fn accept_resource_of(mut self, types: &[ResourceType]) -> Self {
        self.accepted.push(AcceptedForm::Resource {
            types: Some(types.to_vec()),
        });
        self
    }

}

// ============================================================
// Internal parse representation
// ============================================================

#[derive(Debug)]
enum ParsedRef {
    Absolute {
        namespace: String,
        resource_type: Option<ResourceType>,
        name: Option<String>,
    },
    Relative(RelativeRef),
}

#[derive(Debug)]
enum RelativeRef {
    /// Single segment matching a type keyword: `wl`
    TypeOnly(ResourceType),
    /// Single segment not matching a type keyword: `my-app`
    NameOnly(String),
    /// Two segments, first is type keyword: `wl/my-app`
    TypeAndName(ResourceType, String),
}

// ============================================================
// Parse errors (internal, converted to EntityRefError)
// ============================================================

#[derive(Debug)]
enum ParseErrorKind {
    EmptyRef,
    UnknownTypeKeyword {
        word: String,
        /// Other segments from the input, for generating hints.
        other_segments: Vec<String>,
    },
    TooManySegments,
}

// ============================================================
// Public error type
// ============================================================

#[derive(Debug)]
pub struct EntityRefError {
    kind: ErrorKind,
    input: String,
    // Partial resolution info, best-effort, for hint generation.
    namespace: Option<String>,
    resource_type: Option<ResourceType>,
    name: Option<String>,
    // Command context.
    command_name: String,
    accepted: Vec<AcceptedForm>,
    default_type: Option<ResourceType>,
}

#[derive(Debug)]
enum ErrorKind {
    EmptyRef,
    UnknownTypeKeyword {
        word: String,
        other_segments: Vec<String>,
    },
    TooManySegments,
    NoDefaultNamespace,
    NoDefaultType,
    FormNotAccepted {
        got: RefForm,
    },
    TypeNotAccepted {
        got: ResourceType,
        accepted: Vec<ResourceType>,
    },
}

impl std::error::Error for EntityRefError {}

impl fmt::Display for EntityRefError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.write_message(f)?;
        self.write_help(f)
    }
}

impl EntityRefError {
    fn write_message(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ErrorKind::EmptyRef => {
                write!(f, "empty entity reference")
            }
            ErrorKind::UnknownTypeKeyword { word, .. } => {
                write!(f, "unknown resource type \"{word}\"")
            }
            ErrorKind::TooManySegments => {
                write!(f, "too many segments in \"{}\"", self.input)
            }
            ErrorKind::NoDefaultNamespace => {
                write!(f, "no default namespace set")
            }
            ErrorKind::NoDefaultType => {
                write!(
                    f,
                    "`{}` requires a resource type for \"{}\"",
                    self.command_name, self.input
                )
            }
            ErrorKind::FormNotAccepted { got } => {
                write!(
                    f,
                    "`{}` does not accept {}",
                    self.command_name,
                    got.describe()
                )
            }
            ErrorKind::TypeNotAccepted { got, accepted } => {
                let accepted_str = accepted
                    .iter()
                    .map(|t| t.plural())
                    .collect::<Vec<_>>()
                    .join(" or ");
                write!(
                    f,
                    "`{}` operates on {}, not {}",
                    self.command_name,
                    accepted_str,
                    got.plural()
                )
            }
        }
    }

    fn write_help(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ErrorKind::EmptyRef => self.write_accepted_forms(f),

            ErrorKind::UnknownTypeKeyword {
                word,
                other_segments,
            } => {
                write!(f, "\n  Known types: {}", ResourceType::all_keywords_display())?;
                // Suggest the absolute form if the unknown word might be a namespace.
                match other_segments.as_slice() {
                    [name] => write!(
                        f,
                        "\n  Hint: if \"{word}\" is a namespace, use /{word}/type/{name}"
                    ),
                    _ => write!(
                        f,
                        "\n  Hint: if \"{word}\" is a namespace, use /{word}/type/name"
                    ),
                }
            }

            ErrorKind::TooManySegments => {
                write!(f, "\n  Expected format: /namespace/type/name")
            }

            ErrorKind::NoDefaultNamespace => {
                let suffix = self.format_known_suffix();
                write!(
                    f,
                    "\n  Use a fully qualified reference: dv {} /namespace{}",
                    self.command_name, suffix
                )?;
                write!(f, "\n  Or set a default: dv context set-namespace <name>")
            }

            ErrorKind::NoDefaultType => {
                let types = self.collect_accepted_types();
                for t in &types {
                    write!(
                        f,
                        "\n  Try: dv {} {}/{}",
                        self.command_name,
                        t.short(),
                        self.input
                    )?;
                }
                Ok(())
            }

            ErrorKind::FormNotAccepted { .. } => self.write_accepted_forms(f),

            ErrorKind::TypeNotAccepted { accepted, .. } => {
                if let Some(suggestion) = self.substitute_type(accepted[0]) {
                    write!(f, "\n  Hint: dv {} {suggestion}", self.command_name)?;
                }
                Ok(())
            }
        }
    }

    /// Writes the list of accepted forms for this command.
    fn write_accepted_forms(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.accepted.is_empty() {
            return Ok(());
        }
        write!(f, "\n  Accepted forms for `{}`:", self.command_name)?;
        for form in &self.accepted {
            write!(f, "\n    {}", form.format_pattern())?;
        }
        Ok(())
    }

    /// Build a suffix string from the partial resolution state.
    /// Used to suggest a fully-qualified ref when the namespace is missing.
    fn format_known_suffix(&self) -> String {
        match (&self.resource_type, &self.name) {
            (Some(rt), Some(n)) => format!("/{}/{n}", rt.short()),
            (Some(rt), None) => format!("/{}", rt.short()),
            (None, Some(n)) => {
                if let Some(dt) = self.default_type {
                    format!("/{}/{n}", dt.short())
                } else {
                    format!("/type/{n}")
                }
            }
            (None, None) => String::new(),
        }
    }

    /// Substitute the type in the user's input to produce a valid reference.
    fn substitute_type(&self, target: ResourceType) -> Option<String> {
        let ns = self.namespace.as_deref()?;
        let name = self.name.as_deref()?;
        Some(format!("/{ns}/{}/{name}", target.short()))
    }

    /// Collect all accepted types from the spec stored in this error.
    fn collect_accepted_types(&self) -> Vec<ResourceType> {
        let mut types = Vec::new();
        let mut any = false;
        for form in &self.accepted {
            match form.type_constraint() {
                None => any = true,
                Some(ts) => types.extend_from_slice(ts),
            }
        }
        if any {
            return ResourceType::all().to_vec();
        }
        types.sort_by_key(|t| *t as u8);
        types.dedup();
        types
    }
}

// ============================================================
// Parsing
// ============================================================

fn parse(input: &str) -> Result<ParsedRef, ParseErrorKind> {
    if input.is_empty() {
        return Err(ParseErrorKind::EmptyRef);
    }

    if let Some(rest) = input.strip_prefix('/') {
        parse_absolute(rest)
    } else {
        parse_relative(input)
    }
}

fn parse_absolute(input: &str) -> Result<ParsedRef, ParseErrorKind> {
    let segments: Vec<&str> = input.split('/').collect();
    match segments.as_slice() {
        [] | [""] => Err(ParseErrorKind::EmptyRef),

        [ns] => Ok(ParsedRef::Absolute {
            namespace: (*ns).into(),
            resource_type: None,
            name: None,
        }),

        [ns, type_kw] => {
            let rt = parse_type_keyword(type_kw, &[])?;
            Ok(ParsedRef::Absolute {
                namespace: (*ns).into(),
                resource_type: Some(rt),
                name: None,
            })
        }

        [ns, type_kw, name] => {
            let rt = parse_type_keyword(type_kw, &[(*name).into()])?;
            Ok(ParsedRef::Absolute {
                namespace: (*ns).into(),
                resource_type: Some(rt),
                name: Some((*name).into()),
            })
        }

        _ => Err(ParseErrorKind::TooManySegments),
    }
}

fn parse_relative(input: &str) -> Result<ParsedRef, ParseErrorKind> {
    let segments: Vec<&str> = input.split('/').collect();
    match segments.as_slice() {
        [] | [""] => Err(ParseErrorKind::EmptyRef),

        [single] => {
            if let Some(rt) = ResourceType::from_keyword(single) {
                Ok(ParsedRef::Relative(RelativeRef::TypeOnly(rt)))
            } else {
                Ok(ParsedRef::Relative(RelativeRef::NameOnly((*single).into())))
            }
        }

        [type_kw, name] => {
            let rt = parse_type_keyword(type_kw, &[(*name).into()])?;
            Ok(ParsedRef::Relative(RelativeRef::TypeAndName(
                rt,
                (*name).into(),
            )))
        }

        _ => Err(ParseErrorKind::TooManySegments),
    }
}

fn parse_type_keyword(s: &str, other_segments: &[String]) -> Result<ResourceType, ParseErrorKind> {
    ResourceType::from_keyword(s).ok_or_else(|| ParseErrorKind::UnknownTypeKeyword {
        word: s.into(),
        other_segments: other_segments.to_vec(),
    })
}

// ============================================================
// Resolution
// ============================================================

/// Parse and resolve an entity reference string against a command spec.
///
/// This is the primary entry point. It parses the input, fills in
/// defaults (namespace, type), and validates against the spec.
pub fn parse_and_resolve(
    input: &str,
    spec: &EntityRefSpec,
    default_ns: Option<&str>,
) -> Result<ResolvedRef, EntityRefError> {
    let parsed = parse(input).map_err(|e| into_entity_error(e, input, spec, default_ns))?;
    resolve(parsed, input, spec, default_ns)
}

fn resolve(
    parsed: ParsedRef,
    input: &str,
    spec: &EntityRefSpec,
    default_ns: Option<&str>,
) -> Result<ResolvedRef, EntityRefError> {
    // Extract partial info for error context before consuming the parsed ref.
    let partial_ns = match &parsed {
        ParsedRef::Absolute { namespace, .. } => Some(namespace.clone()),
        _ => default_ns.map(String::from),
    };
    let partial_rt = match &parsed {
        ParsedRef::Absolute { resource_type, .. } => *resource_type,
        ParsedRef::Relative(RelativeRef::TypeAndName(rt, _))
        | ParsedRef::Relative(RelativeRef::TypeOnly(rt)) => Some(*rt),
        ParsedRef::Relative(RelativeRef::NameOnly(_)) => spec.default_type,
    };
    let partial_name = match &parsed {
        ParsedRef::Absolute { name, .. } => name.clone(),
        ParsedRef::Relative(RelativeRef::TypeAndName(_, n))
        | ParsedRef::Relative(RelativeRef::NameOnly(n)) => Some(n.clone()),
        ParsedRef::Relative(RelativeRef::TypeOnly(_)) => None,
    };

    let make_err = |kind: ErrorKind| EntityRefError {
        kind,
        input: input.into(),
        namespace: partial_ns.clone(),
        resource_type: partial_rt,
        name: partial_name.clone(),
        command_name: spec.command_name.clone(),
        accepted: spec.accepted.clone(),
        default_type: spec.default_type,
    };

    // Step 1: Fill in defaults to produce a resolved ref.
    let resolved = match parsed {
        ParsedRef::Absolute {
            namespace,
            resource_type: Some(rt),
            name: Some(n),
        } => ResolvedRef::Resource(namespace, rt, n),

        ParsedRef::Absolute {
            namespace,
            resource_type: Some(rt),
            name: None,
        } => ResolvedRef::TypeInNamespace(namespace, rt),

        ParsedRef::Absolute {
            namespace,
            resource_type: None,
            ..
        } => ResolvedRef::Namespace(namespace),

        ParsedRef::Relative(RelativeRef::TypeAndName(rt, n)) => {
            let ns = default_ns
                .ok_or_else(|| make_err(ErrorKind::NoDefaultNamespace))?
                .to_string();
            ResolvedRef::Resource(ns, rt, n)
        }

        ParsedRef::Relative(RelativeRef::TypeOnly(rt)) => {
            let ns = default_ns
                .ok_or_else(|| make_err(ErrorKind::NoDefaultNamespace))?
                .to_string();
            ResolvedRef::TypeInNamespace(ns, rt)
        }

        ParsedRef::Relative(RelativeRef::NameOnly(n)) => {
            let ns = default_ns
                .ok_or_else(|| make_err(ErrorKind::NoDefaultNamespace))?
                .to_string();
            let rt = spec
                .default_type
                .ok_or_else(|| make_err(ErrorKind::NoDefaultType))?;
            ResolvedRef::Resource(ns, rt, n)
        }
    };

    // Step 2: Validate the resolved form against the spec.
    let form = resolved.form();
    let matching = spec.accepted.iter().find(|a| a.matches_form(form));
    let accepted_form =
        matching.ok_or_else(|| make_err(ErrorKind::FormNotAccepted { got: form }))?;

    // Step 3: Validate type constraint.
    if let Some(rt) = resolved.resource_type() {
        if let Some(allowed) = accepted_form.type_constraint() {
            if !allowed.contains(&rt) {
                return Err(make_err(ErrorKind::TypeNotAccepted {
                    got: rt,
                    accepted: allowed.to_vec(),
                }));
            }
        }
    }

    Ok(resolved)
}

/// Convert a parse error into an EntityRefError with command context.
fn into_entity_error(
    err: ParseErrorKind,
    input: &str,
    spec: &EntityRefSpec,
    default_ns: Option<&str>,
) -> EntityRefError {
    let kind = match err {
        ParseErrorKind::EmptyRef => ErrorKind::EmptyRef,
        ParseErrorKind::UnknownTypeKeyword {
            word,
            other_segments,
        } => ErrorKind::UnknownTypeKeyword {
            word,
            other_segments,
        },
        ParseErrorKind::TooManySegments => ErrorKind::TooManySegments,
    };

    EntityRefError {
        kind,
        input: input.into(),
        namespace: default_ns.map(String::from),
        resource_type: None,
        name: None,
        command_name: spec.command_name.clone(),
        accepted: spec.accepted.clone(),
        default_type: spec.default_type,
    }
}

// ============================================================
// Helpers
// ============================================================

fn format_type_placeholder(types: Option<&[ResourceType]>) -> String {
    match types {
        None => "type".into(),
        Some([single]) => single.short().into(),
        Some(ts) => {
            let names: Vec<&str> = ts.iter().map(|t| t.short()).collect();
            format!("{{{}}}", names.join("|"))
        }
    }
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn workload_spec() -> EntityRefSpec {
        EntityRefSpec::new("logs")
            .default_type(ResourceType::Workload)
            .accept_resource_of(&[ResourceType::Workload])
    }

    fn broad_spec() -> EntityRefSpec {
        EntityRefSpec::new("status")
            .accept_namespace()
            .accept_type_any()
            .accept_resource_any()
    }

    fn namespace_only_spec() -> EntityRefSpec {
        EntityRefSpec::new("down").accept_namespace()
    }

    fn no_default_type_spec() -> EntityRefSpec {
        EntityRefSpec::new("get")
            .accept_type_any()
            .accept_resource_any()
    }

    // --- Absolute refs ---

    #[test]
    fn absolute_namespace() {
        let r = parse_and_resolve("/my-ns", &broad_spec(), None).unwrap();
        assert_eq!(r, ResolvedRef::Namespace("my-ns".into()));
    }

    #[test]
    fn absolute_type_in_namespace() {
        let r = parse_and_resolve("/my-ns/wl", &broad_spec(), None).unwrap();
        assert_eq!(
            r,
            ResolvedRef::TypeInNamespace("my-ns".into(), ResourceType::Workload)
        );
    }

    #[test]
    fn absolute_resource() {
        let r = parse_and_resolve("/my-ns/wl/my-app", &broad_spec(), None).unwrap();
        assert_eq!(
            r,
            ResolvedRef::Resource("my-ns".into(), ResourceType::Workload, "my-app".into())
        );
    }

    #[test]
    fn absolute_type_aliases() {
        let r = parse_and_resolve("/ns/svc/foo", &broad_spec(), None).unwrap();
        assert_eq!(
            r,
            ResolvedRef::Resource("ns".into(), ResourceType::Service, "foo".into())
        );

        let r = parse_and_resolve("/ns/po/bar", &broad_spec(), None).unwrap();
        assert_eq!(
            r,
            ResolvedRef::Resource("ns".into(), ResourceType::Pod, "bar".into())
        );
    }

    #[test]
    fn absolute_full_type_names() {
        let r = parse_and_resolve("/ns/workload/app", &broad_spec(), None).unwrap();
        assert_eq!(
            r,
            ResolvedRef::Resource("ns".into(), ResourceType::Workload, "app".into())
        );

        let r = parse_and_resolve("/ns/service/svc1", &broad_spec(), None).unwrap();
        assert_eq!(
            r,
            ResolvedRef::Resource("ns".into(), ResourceType::Service, "svc1".into())
        );
    }

    // --- Relative refs with default namespace ---

    #[test]
    fn relative_type_and_name() {
        let r = parse_and_resolve("wl/my-app", &broad_spec(), Some("default-ns")).unwrap();
        assert_eq!(
            r,
            ResolvedRef::Resource("default-ns".into(), ResourceType::Workload, "my-app".into())
        );
    }

    #[test]
    fn relative_type_only() {
        let r = parse_and_resolve("wl", &broad_spec(), Some("default-ns")).unwrap();
        assert_eq!(
            r,
            ResolvedRef::TypeInNamespace("default-ns".into(), ResourceType::Workload)
        );
    }

    #[test]
    fn relative_bare_name_with_default_type() {
        let r = parse_and_resolve("my-app", &workload_spec(), Some("default-ns")).unwrap();
        assert_eq!(
            r,
            ResolvedRef::Resource(
                "default-ns".into(),
                ResourceType::Workload,
                "my-app".into()
            )
        );
    }

    // --- Error cases ---

    #[test]
    fn error_no_default_namespace() {
        let err = parse_and_resolve("wl/my-app", &broad_spec(), None).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("no default namespace set"), "got: {msg}");
        assert!(msg.contains("dv context set-namespace"), "got: {msg}");
    }

    #[test]
    fn error_no_default_type() {
        let err =
            parse_and_resolve("my-app", &no_default_type_spec(), Some("ns")).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("requires a resource type"),
            "got: {msg}"
        );
        assert!(msg.contains("wl/my-app"), "got: {msg}");
    }

    #[test]
    fn error_unknown_type_keyword() {
        let err = parse_and_resolve("foo/bar", &broad_spec(), Some("ns")).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("unknown resource type \"foo\""), "got: {msg}");
        assert!(msg.contains("Known types:"), "got: {msg}");
        assert!(
            msg.contains("if \"foo\" is a namespace, use /foo/type/bar"),
            "got: {msg}"
        );
    }

    #[test]
    fn error_form_not_accepted_namespace_for_resource_command() {
        let err = parse_and_resolve("/my-ns", &workload_spec(), None).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("does not accept"), "got: {msg}");
        assert!(msg.contains("namespace reference"), "got: {msg}");
    }

    #[test]
    fn error_type_not_accepted() {
        let err = parse_and_resolve("/my-ns/svc/foo", &workload_spec(), None).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("operates on workloads, not services"), "got: {msg}");
        assert!(msg.contains("/my-ns/wl/foo"), "got: {msg}");
    }

    #[test]
    fn error_too_specific_for_namespace_command() {
        let err =
            parse_and_resolve("/my-ns/wl/foo", &namespace_only_spec(), None).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("does not accept"), "got: {msg}");
        assert!(msg.contains("/namespace"), "got: {msg}");
    }

    #[test]
    fn error_too_many_segments() {
        let err = parse_and_resolve("/a/b/c/d", &broad_spec(), None).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("too many segments"), "got: {msg}");
    }

    #[test]
    fn error_empty() {
        let err = parse_and_resolve("", &broad_spec(), None).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("empty entity reference"), "got: {msg}");
    }

    // --- Display formatting ---

    #[test]
    fn resolved_ref_path() {
        assert_eq!(ResolvedRef::Namespace("ns".into()).path(), "/ns");
        assert_eq!(
            ResolvedRef::TypeInNamespace("ns".into(), ResourceType::Workload).path(),
            "/ns/wl"
        );
        assert_eq!(
            ResolvedRef::Resource("ns".into(), ResourceType::Service, "foo".into()).path(),
            "/ns/svc/foo"
        );
    }

    #[test]
    fn resolved_ref_describe() {
        assert_eq!(
            ResolvedRef::Namespace("staging".into()).describe(),
            "namespace \"staging\""
        );
        assert_eq!(
            ResolvedRef::TypeInNamespace("staging".into(), ResourceType::Workload).describe(),
            "workloads in namespace \"staging\""
        );
        assert_eq!(
            ResolvedRef::Resource("staging".into(), ResourceType::Workload, "api".into())
                .describe(),
            "workload \"staging/api\""
        );
    }
}
