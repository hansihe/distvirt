/// Label selector expressions, modeled after Kubernetes label selectors.
///
/// Syntax:
/// ```text
/// env=staging              # equality
/// env!=production          # inequality
/// env in (staging,dev)     # set membership
/// env notin (production)   # set exclusion
/// env                      # key exists
/// !env                     # key doesn't exist
/// ```
///
/// Multiple predicates are comma-separated (implicit AND):
/// ```text
/// env=staging,team=platform
/// ```
use std::fmt;

/// A parsed selector composed of predicates that all must match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Selector {
    predicates: Vec<Predicate>,
}

/// A single predicate within a selector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Predicate {
    Equals { key: String, value: String },
    NotEquals { key: String, value: String },
    In { key: String, values: Vec<String> },
    NotIn { key: String, values: Vec<String> },
    Exists { key: String },
    NotExists { key: String },
}

/// Error returned when parsing a selector expression fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    pub message: String,
    pub position: usize,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "at position {}: {}", self.position, self.message)
    }
}

impl std::error::Error for ParseError {}

impl Selector {
    /// Parse a selector expression string.
    pub fn parse(input: &str) -> Result<Self, ParseError> {
        Parser::new(input).parse_selector()
    }

    /// An empty selector that matches everything.
    pub fn empty() -> Self {
        Selector {
            predicates: Vec::new(),
        }
    }

    /// Returns true if this selector has no predicates (matches everything).
    pub fn is_empty(&self) -> bool {
        self.predicates.is_empty()
    }

    /// Test whether a set of key-value pairs matches all predicates.
    ///
    /// The `lookup` closure is called with a key and should return the
    /// corresponding value if it exists. This decouples the selector
    /// engine from any particular storage representation.
    pub fn matches<'a>(&self, lookup: impl Fn(&str) -> Option<&'a str>) -> bool {
        self.predicates.iter().all(|p| p.matches(&lookup))
    }

    /// Access the individual predicates.
    pub fn predicates(&self) -> &[Predicate] {
        &self.predicates
    }
}

impl Predicate {
    fn matches<'a>(&self, lookup: &impl Fn(&str) -> Option<&'a str>) -> bool {
        match self {
            Predicate::Equals { key, value } => {
                matches!(lookup(key), Some(v) if v == value)
            }
            Predicate::NotEquals { key, value } => {
                !matches!(lookup(key), Some(v) if v == value)
            }
            Predicate::In { key, values } => match lookup(key) {
                Some(v) => values.iter().any(|candidate| candidate.as_str() == v),
                None => false,
            },
            Predicate::NotIn { key, values } => match lookup(key) {
                Some(v) => !values.iter().any(|candidate| candidate.as_str() == v),
                None => true,
            },
            Predicate::Exists { key } => lookup(key).is_some(),
            Predicate::NotExists { key } => lookup(key).is_none(),
        }
    }
}

impl fmt::Display for Selector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, p) in self.predicates.iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            write!(f, "{}", p)?;
        }
        Ok(())
    }
}

impl fmt::Display for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Predicate::Equals { key, value } => write!(f, "{}={}", key, value),
            Predicate::NotEquals { key, value } => write!(f, "{}!={}", key, value),
            Predicate::In { key, values } => {
                write!(f, "{} in ({})", key, values.join(","))
            }
            Predicate::NotIn { key, values } => {
                write!(f, "{} notin ({})", key, values.join(","))
            }
            Predicate::Exists { key } => write!(f, "{}", key),
            Predicate::NotExists { key } => write!(f, "!{}", key),
        }
    }
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

struct Parser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Parser { input, pos: 0 }
    }

    fn parse_selector(mut self) -> Result<Selector, ParseError> {
        let mut predicates = Vec::new();

        self.skip_whitespace();
        if self.pos >= self.input.len() {
            return Ok(Selector::empty());
        }

        predicates.push(self.parse_predicate()?);

        loop {
            self.skip_whitespace();
            if self.pos >= self.input.len() {
                break;
            }
            if self.peek() != Some(',') {
                return Err(self.error("expected ',' or end of input"));
            }
            self.advance(); // consume ','
            self.skip_whitespace();
            if self.pos >= self.input.len() {
                return Err(self.error("unexpected end of input after ','"));
            }
            predicates.push(self.parse_predicate()?);
        }

        Ok(Selector { predicates })
    }

    fn parse_predicate(&mut self) -> Result<Predicate, ParseError> {
        self.skip_whitespace();

        // Check for negation prefix (existence check)
        if self.peek() == Some('!') {
            self.advance();
            let key = self.parse_key()?;
            return Ok(Predicate::NotExists { key });
        }

        let key = self.parse_key()?;

        self.skip_whitespace();

        // What follows the key determines the predicate type
        match self.peek() {
            // End of input or comma → existence check
            None | Some(',') => return Ok(Predicate::Exists { key }),

            // != operator
            Some('!') => {
                self.advance();
                if self.peek() != Some('=') {
                    return Err(self.error("expected '=' after '!'"));
                }
                self.advance();
                self.skip_whitespace();
                let value = self.parse_value()?;
                return Ok(Predicate::NotEquals { key, value });
            }

            // = operator
            Some('=') => {
                self.advance();
                self.skip_whitespace();
                let value = self.parse_value()?;
                return Ok(Predicate::Equals { key, value });
            }

            // Possibly "in" or "notin" keyword
            _ => {}
        }

        // Check for "in"/"notin" — the key we parsed might actually be
        // "key<whitespace>in" which we've already split because we stopped
        // at whitespace. So `key` is correct and we need to look for
        // the keyword now.
        let remaining = &self.input[self.pos..];

        if remaining.starts_with("in")
            && remaining[2..]
                .chars()
                .next()
                .map_or(true, |c| c == ' ' || c == '(')
        {
            self.pos += 2;
            self.skip_whitespace();
            let values = self.parse_value_set()?;
            return Ok(Predicate::In { key, values });
        }

        if remaining.starts_with("notin")
            && remaining[5..]
                .chars()
                .next()
                .map_or(true, |c| c == ' ' || c == '(')
        {
            self.pos += 5;
            self.skip_whitespace();
            let values = self.parse_value_set()?;
            return Ok(Predicate::NotIn { key, values });
        }

        Err(self.error("expected operator: '=', '!=', 'in', or 'notin'"))
    }

    fn parse_key(&mut self) -> Result<String, ParseError> {
        let start = self.pos;
        while self.pos < self.input.len() {
            let c = self.input.as_bytes()[self.pos] as char;
            if c.is_alphanumeric() || c == '_' || c == '-' || c == '.' || c == '/' {
                self.pos += 1;
            } else {
                break;
            }
        }
        if self.pos == start {
            return Err(self.error("expected key"));
        }
        Ok(self.input[start..self.pos].to_string())
    }

    fn parse_value(&mut self) -> Result<String, ParseError> {
        let start = self.pos;
        while self.pos < self.input.len() {
            let c = self.input.as_bytes()[self.pos] as char;
            if c.is_alphanumeric() || c == '_' || c == '-' || c == '.' {
                self.pos += 1;
            } else {
                break;
            }
        }
        // Empty values are allowed (e.g. `key=` means key exists with empty value)
        Ok(self.input[start..self.pos].to_string())
    }

    fn parse_value_set(&mut self) -> Result<Vec<String>, ParseError> {
        if self.peek() != Some('(') {
            return Err(self.error("expected '(' to start value set"));
        }
        self.advance();

        let mut values = Vec::new();
        self.skip_whitespace();

        if self.peek() == Some(')') {
            self.advance();
            return Ok(values);
        }

        values.push(self.parse_value()?);

        loop {
            self.skip_whitespace();
            match self.peek() {
                Some(')') => {
                    self.advance();
                    return Ok(values);
                }
                Some(',') => {
                    self.advance();
                    self.skip_whitespace();
                    values.push(self.parse_value()?);
                }
                _ => return Err(self.error("expected ',' or ')' in value set")),
            }
        }
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.input.len() && self.input.as_bytes()[self.pos] == b' ' {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<char> {
        self.input.as_bytes().get(self.pos).map(|&b| b as char)
    }

    fn advance(&mut self) {
        self.pos += 1;
    }

    fn error(&self, message: &str) -> ParseError {
        ParseError {
            message: message.to_string(),
            position: self.pos,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_labels(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn matches_labels(selector: &Selector, labels: &HashMap<String, String>) -> bool {
        selector.matches(move |k| labels.get(k).map(|v| v.as_str()))
    }

    // -- Parsing tests --

    #[test]
    fn parse_empty() {
        let sel = Selector::parse("").unwrap();
        assert!(sel.is_empty());
    }

    #[test]
    fn parse_equals() {
        let sel = Selector::parse("env=staging").unwrap();
        assert_eq!(
            sel.predicates(),
            &[Predicate::Equals {
                key: "env".into(),
                value: "staging".into()
            }]
        );
    }

    #[test]
    fn parse_not_equals() {
        let sel = Selector::parse("env!=production").unwrap();
        assert_eq!(
            sel.predicates(),
            &[Predicate::NotEquals {
                key: "env".into(),
                value: "production".into()
            }]
        );
    }

    #[test]
    fn parse_in() {
        let sel = Selector::parse("env in (staging,dev)").unwrap();
        assert_eq!(
            sel.predicates(),
            &[Predicate::In {
                key: "env".into(),
                values: vec!["staging".into(), "dev".into()]
            }]
        );
    }

    #[test]
    fn parse_notin() {
        let sel = Selector::parse("env notin (production)").unwrap();
        assert_eq!(
            sel.predicates(),
            &[Predicate::NotIn {
                key: "env".into(),
                values: vec!["production".into()]
            }]
        );
    }

    #[test]
    fn parse_exists() {
        let sel = Selector::parse("env").unwrap();
        assert_eq!(
            sel.predicates(),
            &[Predicate::Exists { key: "env".into() }]
        );
    }

    #[test]
    fn parse_not_exists() {
        let sel = Selector::parse("!env").unwrap();
        assert_eq!(
            sel.predicates(),
            &[Predicate::NotExists { key: "env".into() }]
        );
    }

    #[test]
    fn parse_multiple() {
        let sel = Selector::parse("env=staging,team=platform,!deprecated").unwrap();
        assert_eq!(sel.predicates().len(), 3);
        assert_eq!(
            sel.predicates()[0],
            Predicate::Equals {
                key: "env".into(),
                value: "staging".into()
            }
        );
        assert_eq!(
            sel.predicates()[1],
            Predicate::Equals {
                key: "team".into(),
                value: "platform".into()
            }
        );
        assert_eq!(
            sel.predicates()[2],
            Predicate::NotExists {
                key: "deprecated".into()
            }
        );
    }

    #[test]
    fn parse_whitespace_tolerance() {
        let sel = Selector::parse("  env = staging , team != infra  ").unwrap();
        assert_eq!(sel.predicates().len(), 2);
        assert_eq!(
            sel.predicates()[0],
            Predicate::Equals {
                key: "env".into(),
                value: "staging".into()
            }
        );
        assert_eq!(
            sel.predicates()[1],
            Predicate::NotEquals {
                key: "team".into(),
                value: "infra".into()
            }
        );
    }

    #[test]
    fn parse_in_whitespace() {
        let sel = Selector::parse("env in ( staging , dev )").unwrap();
        assert_eq!(
            sel.predicates(),
            &[Predicate::In {
                key: "env".into(),
                values: vec!["staging".into(), "dev".into()]
            }]
        );
    }

    #[test]
    fn parse_empty_value_set() {
        let sel = Selector::parse("env in ()").unwrap();
        assert_eq!(
            sel.predicates(),
            &[Predicate::In {
                key: "env".into(),
                values: vec![]
            }]
        );
    }

    #[test]
    fn parse_key_with_dots_and_slashes() {
        let sel = Selector::parse("app.kubernetes.io/name=myapp").unwrap();
        assert_eq!(
            sel.predicates(),
            &[Predicate::Equals {
                key: "app.kubernetes.io/name".into(),
                value: "myapp".into()
            }]
        );
    }

    #[test]
    fn parse_error_trailing_comma() {
        assert!(Selector::parse("env=staging,").is_err());
    }

    #[test]
    fn parse_error_bad_operator() {
        assert!(Selector::parse("env~staging").is_err());
    }

    // -- Matching tests --

    #[test]
    fn match_equals() {
        let sel = Selector::parse("env=staging").unwrap();
        let labels = make_labels(&[("env", "staging")]);
        assert!(matches_labels(&sel, &labels));

        let labels = make_labels(&[("env", "production")]);
        assert!(!matches_labels(&sel, &labels));

        let labels = make_labels(&[]);
        assert!(!matches_labels(&sel, &labels));
    }

    #[test]
    fn match_not_equals() {
        let sel = Selector::parse("env!=production").unwrap();
        let labels = make_labels(&[("env", "staging")]);
        assert!(matches_labels(&sel, &labels));

        let labels = make_labels(&[("env", "production")]);
        assert!(!matches_labels(&sel, &labels));

        // Key missing: lookup returns None, which != Some("production") → true
        let labels = make_labels(&[]);
        assert!(matches_labels(&sel, &labels));
    }

    #[test]
    fn match_in() {
        let sel = Selector::parse("env in (staging,dev)").unwrap();

        assert!(matches_labels(&sel, &make_labels(&[("env", "staging")])));
        assert!(matches_labels(&sel, &make_labels(&[("env", "dev")])));
        assert!(!matches_labels(
            &sel,
            &make_labels(&[("env", "production")])
        ));
        assert!(!matches_labels(&sel, &make_labels(&[])));
    }

    #[test]
    fn match_notin() {
        let sel = Selector::parse("env notin (production)").unwrap();

        assert!(matches_labels(&sel, &make_labels(&[("env", "staging")])));
        assert!(!matches_labels(
            &sel,
            &make_labels(&[("env", "production")])
        ));
        // Key missing → not in the set → true
        assert!(matches_labels(&sel, &make_labels(&[])));
    }

    #[test]
    fn match_exists() {
        let sel = Selector::parse("env").unwrap();
        assert!(matches_labels(&sel, &make_labels(&[("env", "anything")])));
        assert!(!matches_labels(&sel, &make_labels(&[])));
    }

    #[test]
    fn match_not_exists() {
        let sel = Selector::parse("!env").unwrap();
        assert!(!matches_labels(&sel, &make_labels(&[("env", "anything")])));
        assert!(matches_labels(&sel, &make_labels(&[])));
    }

    #[test]
    fn match_multiple_predicates_and_semantics() {
        let sel = Selector::parse("env=staging,team=platform").unwrap();

        // Both match
        assert!(matches_labels(
            &sel,
            &make_labels(&[("env", "staging"), ("team", "platform")])
        ));

        // Only one matches
        assert!(!matches_labels(
            &sel,
            &make_labels(&[("env", "staging"), ("team", "infra")])
        ));

        // Extra labels are fine
        assert!(matches_labels(
            &sel,
            &make_labels(&[("env", "staging"), ("team", "platform"), ("version", "2")])
        ));
    }

    #[test]
    fn empty_selector_matches_everything() {
        let sel = Selector::empty();
        assert!(matches_labels(&sel, &make_labels(&[])));
        assert!(matches_labels(
            &sel,
            &make_labels(&[("anything", "goes")])
        ));
    }

    // -- Display roundtrip --

    #[test]
    fn display_roundtrip() {
        let cases = &[
            "env=staging",
            "env!=production",
            "env in (staging,dev)",
            "env notin (production)",
            "env",
            "!env",
            "env=staging,team=platform,!deprecated",
        ];

        for &input in cases {
            let sel = Selector::parse(input).unwrap();
            let rendered = sel.to_string();
            let reparsed = Selector::parse(&rendered).unwrap();
            assert_eq!(sel, reparsed, "roundtrip failed for: {}", input);
        }
    }
}
