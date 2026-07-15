use std::fmt;

use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use thiserror::Error;
use zeroize::Zeroizing;

const MIN_ADMIN_TOKEN_BYTES: usize = 32;
const MAX_ADMIN_TOKEN_BYTES: usize = 256;

#[derive(Clone)]
pub struct AdminTokenVerifier {
    expected_digest: [u8; 32],
}

impl AdminTokenVerifier {
    pub fn from_secret(secret: Zeroizing<String>) -> Result<Self, AdminTokenError> {
        validate_token(secret.as_bytes())?;
        Ok(Self {
            expected_digest: Sha256::digest(secret.as_bytes()).into(),
        })
    }

    pub fn verify(&self, candidate: &str) -> bool {
        let candidate_digest: [u8; 32] = Sha256::digest(candidate.as_bytes()).into();
        let shape_is_valid = validate_token(candidate.as_bytes()).is_ok() as u8;
        bool::from(self.expected_digest.ct_eq(&candidate_digest)) && shape_is_valid == 1
    }
}

impl fmt::Debug for AdminTokenVerifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AdminTokenVerifier([REDACTED])")
    }
}

fn validate_token(token: &[u8]) -> Result<(), AdminTokenError> {
    if token.len() < MIN_ADMIN_TOKEN_BYTES {
        return Err(AdminTokenError::TooShort);
    }
    if token.len() > MAX_ADMIN_TOKEN_BYTES {
        return Err(AdminTokenError::TooLong);
    }
    if !token.iter().copied().all(is_bearer_token_byte) {
        return Err(AdminTokenError::InvalidCharacter);
    }
    Ok(())
}

fn is_bearer_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~' | b'+' | b'/' | b'=')
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AdminTokenError {
    #[error("admin token must contain at least 32 bytes")]
    TooShort,
    #[error("admin token must contain at most 256 bytes")]
    TooLong,
    #[error("admin token contains a character that is not valid in a bearer token")]
    InvalidCharacter,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(value: &str) -> Zeroizing<String> {
        Zeroizing::new(value.to_owned())
    }

    #[test]
    fn exact_token_is_accepted_and_near_matches_are_rejected() {
        let verifier =
            AdminTokenVerifier::from_secret(token("zN5Vh8cnwT-NfY2M8N1oFhNtvxZ7AS-fBk4B8I3IRXY"))
                .unwrap();

        assert!(verifier.verify("zN5Vh8cnwT-NfY2M8N1oFhNtvxZ7AS-fBk4B8I3IRXY"));
        assert!(!verifier.verify("zN5Vh8cnwT-NfY2M8N1oFhNtvxZ7AS-fBk4B8I3IRXx"));
        assert!(!verifier.verify("short"));
        assert!(!verifier.verify("zN5Vh8cnwT NfY2M8N1oFhNtvxZ7AS-fBk4B8I3IRXY"));
    }

    #[test]
    fn weak_or_header_unsafe_tokens_are_rejected_at_configuration_time() {
        assert_eq!(
            AdminTokenVerifier::from_secret(token("too-short")).unwrap_err(),
            AdminTokenError::TooShort
        );
        assert_eq!(
            AdminTokenVerifier::from_secret(token(&"a".repeat(257))).unwrap_err(),
            AdminTokenError::TooLong
        );
        assert_eq!(
            AdminTokenVerifier::from_secret(token(&format!("{}!", "a".repeat(31)))).unwrap_err(),
            AdminTokenError::InvalidCharacter
        );
    }

    #[test]
    fn debug_output_never_contains_the_token_or_digest() {
        let secret = "zN5Vh8cnwT-NfY2M8N1oFhNtvxZ7AS-fBk4B8I3IRXY";
        let verifier = AdminTokenVerifier::from_secret(token(secret)).unwrap();
        let debug = format!("{verifier:?}");

        assert_eq!(debug, "AdminTokenVerifier([REDACTED])");
        assert!(!debug.contains(secret));
        assert!(!debug.contains(&hex::encode(Sha256::digest(secret.as_bytes()))));
    }
}
