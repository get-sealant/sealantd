//! Secret redaction for captured I/O (plan §18). Masks configured literal secrets (e.g. the values
//! of secret-looking env vars) and high-confidence token shapes, never silently — each masked span
//! is counted and surfaced as `transform.redacted` + the `redactedEvents` health counter.

const REDACTED: &[u8] = b"***REDACTED***";

/// High-confidence secret token prefixes. A match is redacted only when the full token reaches
/// [`MIN_TOKEN_LEN`], to avoid masking ordinary words.
const TOKEN_PREFIXES: &[&[u8]] = &[
    b"sk-",
    b"ghp_",
    b"gho_",
    b"ghs_",
    b"github_pat_",
    b"AKIA",
    b"xoxb-",
    b"xoxp-",
    b"glpat-",
    b"AIza",
];

const MIN_TOKEN_LEN: usize = 16;

/// Redacts secrets from byte streams.
#[derive(Debug, Default, Clone)]
pub struct Redactor {
    literals: Vec<Vec<u8>>,
}

impl Redactor {
    /// Build a redactor that masks the given literal secrets (in addition to built-in token shapes).
    /// Literals shorter than 6 bytes are ignored to avoid masking common substrings.
    #[must_use]
    pub fn new(literals: Vec<String>) -> Self {
        let literals = literals
            .into_iter()
            .filter(|l| l.len() >= 6)
            .map(String::into_bytes)
            .collect();
        Self { literals }
    }

    fn is_token_char(b: u8) -> bool {
        b.is_ascii_alphanumeric() || b == b'-' || b == b'_'
    }

    /// Redact secrets in `input`. Returns the (possibly unchanged) bytes and the number of masked
    /// spans.
    #[must_use]
    pub fn redact(&self, input: &[u8]) -> (Vec<u8>, u32) {
        let mut out = Vec::with_capacity(input.len());
        let mut count = 0u32;
        let mut i = 0;
        'outer: while i < input.len() {
            for literal in &self.literals {
                if input[i..].starts_with(literal) {
                    out.extend_from_slice(REDACTED);
                    i += literal.len();
                    count += 1;
                    continue 'outer;
                }
            }
            for prefix in TOKEN_PREFIXES {
                if input[i..].starts_with(prefix) {
                    let mut end = i + prefix.len();
                    while end < input.len() && Self::is_token_char(input[end]) {
                        end += 1;
                    }
                    if end - i >= MIN_TOKEN_LEN {
                        out.extend_from_slice(REDACTED);
                        i = end;
                        count += 1;
                        continue 'outer;
                    }
                }
            }
            out.push(input[i]);
            i += 1;
        }
        (out, count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_known_token_shapes() {
        let r = Redactor::default();
        let (out, n) = r.redact(b"using sk-abcdef012345678901234567 now");
        assert_eq!(n, 1);
        assert!(!String::from_utf8_lossy(&out).contains("sk-abcdef"));
        assert!(String::from_utf8_lossy(&out).contains("***REDACTED***"));
        assert!(String::from_utf8_lossy(&out).starts_with("using "));
    }

    #[test]
    fn redacts_configured_literal_secret() {
        let r = Redactor::new(vec!["super-secret-value-123".to_owned()]);
        let (out, n) = r.redact(b"TOKEN=super-secret-value-123\n");
        assert_eq!(n, 1);
        assert!(!String::from_utf8_lossy(&out).contains("super-secret-value-123"));
    }

    #[test]
    fn leaves_ordinary_text_and_short_tokens_alone() {
        let r = Redactor::default();
        let (out, n) = r.redact(b"hello world, sk-short");
        assert_eq!(n, 0);
        assert_eq!(out, b"hello world, sk-short");
    }

    #[test]
    fn counts_multiple_redactions_and_is_binary_safe() {
        let r = Redactor::new(vec!["literalsecret".to_owned()]);
        let mut input = b"\x00\xffliteralsecret AKIA0123456789ABCDEF\x00".to_vec();
        input.extend_from_slice(b" ghp_0123456789abcdefghij");
        let (_out, n) = r.redact(&input);
        assert_eq!(n, 3);
    }
}
